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

import com.google.common.io.CountingOutputStream;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.palantir.logsafe.SafeArg;
import com.palantir.spark.shuffle.async.s3.merger.FileMerger;
import com.palantir.spark.shuffle.async.s3.metrics.MergingShuffleClientMetrics;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DefaultShuffleFileBatchUploader implements ShuffleFileBatchUploader {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultShuffleFileBatchUploader.class);

    private final String appId;
    private final ShuffleDriverEndpointRef shuffleDriverEndpointRef;
    private final MergingShuffleFiles shuffleFiles;
    private final ListeningExecutorService uploadExecutor;
    private final MergingShuffleClientMetrics metrics;
    private final Clock clock;

    public DefaultShuffleFileBatchUploader(
            String appId,
            ShuffleDriverEndpointRef shuffleDriverEndpointRef,
            MergingShuffleFiles shuffleFiles,
            ListeningExecutorService uploadExecutor,
            MergingShuffleClientMetrics metrics,
            Clock clock) {
        this.appId = appId;
        this.shuffleDriverEndpointRef = shuffleDriverEndpointRef;
        this.shuffleFiles = shuffleFiles;
        this.uploadExecutor = uploadExecutor;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public void submitBatchUpload(int shuffleId, ShuffleMapInputBatch batch) {
        ListenableFuture<?> uploadTask = uploadExecutor.submit(() -> {
            long timeSubmitted = batch.earliestInputTimestamp();
            if (shuffleDriverEndpointRef.isShuffleRegistered(shuffleId)) {
                long mergedOutputId = shuffleDriverEndpointRef.getNextMergeId();
                ShuffleMapInput[] inputs = batch.inputBatch();
                uploadMergedData(shuffleId, mergedOutputId, inputs, timeSubmitted);
                uploadMergedIndices(shuffleId, mergedOutputId, inputs, timeSubmitted);

                shuffleDriverEndpointRef.registerMergedMapOutput(
                        Arrays.stream(inputs).map(ShuffleMapInput::mapOutputId).collect(Collectors.toList()),
                        mergedOutputId);
            }
        });
        Futures.addCallback(uploadTask, new UploadBatchFinishedListener(appId, batch), uploadExecutor);
    }

    private void uploadMergedIndices(int shuffleId, long mergedOutputId, ShuffleMapInput[] inputs, long timeSubmitted) {
        metrics.markUploadStarted(shuffleId, mergedOutputId, "index");
        long startTime = clock.millis();
        long bytesWritten;
        CountingOutputStream resolvedCountingIndexOut = null;
        try (OutputStream mergedOut = shuffleFiles.createRemoteMergedIndexFile(mergedOutputId);
                CountingOutputStream countingIndexOut = new CountingOutputStream(mergedOut);
                DataOutputStream mergedDataOut = new DataOutputStream(countingIndexOut)) {
            FileMerger.mergeMapOutputs(inputs, mergedDataOut, ShuffleMapInput::indexSizedInput);
            resolvedCountingIndexOut = countingIndexOut;
        } catch (IOException e) {
            metrics.markUploadFailed(shuffleId, mergedOutputId, "index");
            throw new RuntimeException(e);
        } finally {
            bytesWritten = resolvedCountingIndexOut == null ? 0L : resolvedCountingIndexOut.getCount();
        }
        long now = clock.millis();
        metrics.markUploadCompleted(
                shuffleId, mergedOutputId, "index", bytesWritten, now - startTime, now - timeSubmitted);
    }

    private void uploadMergedData(int shuffleId, long mergedOutputId, ShuffleMapInput[] inputs, long timeSubmitted) {
        metrics.markUploadStarted(shuffleId, mergedOutputId, "data");
        long startTime = clock.millis();
        long bytesWritten;
        CountingOutputStream resolvedCountingDataOut = null;
        try (OutputStream mergedOut = shuffleFiles.createRemoteMergedDataFile(mergedOutputId);
                CountingOutputStream countingDataOut = new CountingOutputStream(mergedOut);
                DataOutputStream mergedDataOut = new DataOutputStream(countingDataOut)) {
            FileMerger.mergeMapOutputs(inputs, mergedDataOut, ShuffleMapInput::dataSizedInput);
            resolvedCountingDataOut = countingDataOut;
        } catch (IOException e) {
            metrics.markUploadFailed(shuffleId, mergedOutputId, "data");
            throw new RuntimeException(e);
        } finally {
            bytesWritten = resolvedCountingDataOut == null ? 0L : resolvedCountingDataOut.getCount();
        }
        long now = clock.millis();
        metrics.markUploadCompleted(
                shuffleId, mergedOutputId, "data", bytesWritten, clock.millis() - startTime, now - timeSubmitted);
    }

    private static final class UploadBatchFinishedListener implements FutureCallback<Object> {
        private final String appId;
        private final ShuffleMapInputBatch batch;

        UploadBatchFinishedListener(String appId, ShuffleMapInputBatch batch) {
            this.batch = batch;
            this.appId = appId;
        }

        @Override
        public void onSuccess(Object _result) {
            LOGGER.debug("Finished uploading batch of map task files.",
                    SafeArg.of("appId", appId),
                    SafeArg.of("uploadedMapOutputs", Arrays.asList(batch.mapOutputIds())),
                    SafeArg.of("uploadSize", batch.totalDataSizeInBytes()));
        }

        @Override
        public void onFailure(Throwable error) {
            LOGGER.error("Failed to upload shuffle files to the backing file system. If this executor fails, shuffle"
                            + " data will need to be recomputed.",
                    SafeArg.of("appId", appId),
                    SafeArg.of("failedMapOutputs", Arrays.asList(batch.mapOutputIds())),
                    SafeArg.of("batchDataSize", batch.totalDataSizeInBytes()),
                    SafeArg.of("numBatchItems", batch.inputBatch().length),
                    error);
        }
    }
}
