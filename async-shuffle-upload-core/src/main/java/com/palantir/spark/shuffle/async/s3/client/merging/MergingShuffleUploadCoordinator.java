/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.spark.shuffle.async.s3.client.merging;

import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import java.time.Clock;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MergingShuffleUploadCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(MergingShuffleUploadCoordinator.class);

    private final long maxBatchSizeBytes;
    private final int maxBufferedInputs;
    private final long maxBatchAgeMillis;
    private final long pollingIntervalMillis;
    // Mapped to the timestamp at which point the input was inserted into the map.
    private final Map<ShuffleMapInput, Long> ungroupedMapInputs;
    private final ShuffleDriverEndpointRef shuffleDriverEndpointRef;
    private final Map<Integer, ShuffleMapInputBatch> stagedBatches;
    private final ListeningScheduledExecutorService uploadCoordinatorExecutor;
    private final Clock clock;
    private final ShuffleFileBatchUploader batchUploader;

    private ListenableScheduledFuture<?> uploadCoordinatorTask;
    private int totalStagedInputs;

    public MergingShuffleUploadCoordinator(
            long maxBatchSizeBytes,
            long maxBatchAgeMillis,
            int maxBufferedInputs,
            long pollingIntervalMillis,
            ShuffleDriverEndpointRef shuffleDriverEndpointRef,
            ListeningScheduledExecutorService uploadCoordinatorExecutor,
            Clock clock,
            ShuffleFileBatchUploader batchUploader) {
        this.maxBatchSizeBytes = maxBatchSizeBytes;
        this.maxBatchAgeMillis = maxBatchAgeMillis;
        this.maxBufferedInputs = maxBufferedInputs;
        this.pollingIntervalMillis = pollingIntervalMillis;
        this.shuffleDriverEndpointRef = shuffleDriverEndpointRef;
        this.stagedBatches = new HashMap<>();
        this.uploadCoordinatorExecutor = uploadCoordinatorExecutor;
        this.clock = clock;
        this.batchUploader = batchUploader;
        this.ungroupedMapInputs = new ConcurrentHashMap<>();
        this.totalStagedInputs = 0;
    }

    public void start() {
        if (uploadCoordinatorTask == null) {
            uploadCoordinatorTask = uploadCoordinatorExecutor.scheduleWithFixedDelay(
                    this::uploadEligibleBatches,
                    pollingIntervalMillis,
                    pollingIntervalMillis,
                    TimeUnit.MILLISECONDS);
        }
    }

    public void stop() {
        if (uploadCoordinatorTask != null) {
            uploadCoordinatorTask.cancel(true);
            uploadCoordinatorTask = null;
        }
    }

    public void addShuffleMapInputForUpload(ShuffleMapInput inputForUpload) {
        ungroupedMapInputs.put(inputForUpload, clock.millis());
    }

    private void uploadEligibleBatches() {
        try {
            Iterator<Map.Entry<ShuffleMapInput, Long>> viewIterator = ungroupedMapInputs.entrySet().iterator();
            while (viewIterator.hasNext()) {
                Map.Entry<ShuffleMapInput, Long> nextInputWithTimestamp = viewIterator.next();
                int shuffleId = nextInputWithTimestamp.getKey().mapOutputId().shuffleId();
                if (shuffleDriverEndpointRef.isShuffleRegistered(shuffleId)) {
                    ShuffleMapInputBatch updatedBatch = stagedBatches.computeIfAbsent(
                            shuffleId,
                            ignored -> new ShuffleMapInputBatch())
                            .addInput(nextInputWithTimestamp.getKey(), nextInputWithTimestamp.getValue());
                    totalStagedInputs++;
                    if (updatedBatch.totalDataSizeInBytes() >= maxBatchSizeBytes) {
                        batchUploader.submitBatchUpload(shuffleId, updatedBatch);
                        removeBatch(shuffleId);
                    }
                } else {
                    removeBatch(shuffleId);
                }
                viewIterator.remove();
            }
            Iterator<Map.Entry<Integer, ShuffleMapInputBatch>> stagedBatchesIt =
                    stagedBatches.entrySet().iterator();
            // This is a hedge against an unbounded growth in the staged inputs, where we get a lot of tiny inputs
            // in rapid succession such that waiting for the time-based trigger will cause the data structure to
            // blow up the heap.
            boolean forceUploadAll = totalStagedInputs > maxBufferedInputs;
            while (stagedBatchesIt.hasNext()) {
                Map.Entry<Integer, ShuffleMapInputBatch> stagedBatchEntry = stagedBatchesIt.next();
                int shuffleId = stagedBatchEntry.getKey();
                ShuffleMapInputBatch stagedBatch = stagedBatchEntry.getValue();
                if (!shuffleDriverEndpointRef.isShuffleRegistered(shuffleId)) {
                    removeBatch(stagedBatchesIt, stagedBatch);
                } else if (forceUploadAll || (stagedBatch.inputBatch().length > 0
                        && clock.millis() - stagedBatch.earliestInputTimestamp()
                        >= maxBatchAgeMillis)) {
                    batchUploader.submitBatchUpload(shuffleId, stagedBatch);
                    removeBatch(stagedBatchesIt, stagedBatch);
                }
            }
            LOGGER.trace("Finished checking for uploading shuffle files to the backing store.");
        } catch (Exception e) {
            LOGGER.warn("Failed to process outstanding uploads for shuffle files.", e);
        }
    }

    private void removeBatch(int shuffleId) {
        Optional<ShuffleMapInputBatch> removedBatch = Optional.ofNullable(stagedBatches.remove(shuffleId));
        removedBatch.ifPresent(batch -> totalStagedInputs -= batch.inputBatch().length);
    }

    private void removeBatch(Iterator<Map.Entry<Integer, ShuffleMapInputBatch>> it, ShuffleMapInputBatch removedBatch) {
        it.remove();
        totalStagedInputs -= removedBatch.inputBatch().length;
    }
}
