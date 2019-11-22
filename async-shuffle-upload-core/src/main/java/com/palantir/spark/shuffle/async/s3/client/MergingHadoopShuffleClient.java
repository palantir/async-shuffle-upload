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

package com.palantir.spark.shuffle.async.s3.client;

import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.spark.shuffle.async.s3.client.merging.MergingShuffleFiles;
import com.palantir.spark.shuffle.async.s3.client.merging.MergingShuffleUploadCoordinator;
import com.palantir.spark.shuffle.async.s3.client.merging.ShuffleMapInput;
import com.palantir.spark.shuffle.async.s3.io.PartitionDecoder;
import com.palantir.spark.shuffle.async.s3.merger.FileMerger;
import com.palantir.spark.shuffle.async.s3.metadata.MapOutputId;
import com.palantir.spark.shuffle.async.s3.metrics.MergingShuffleClientMetrics;
import com.palantir.spark.shuffle.async.s3.util.EmptySizedInput;
import com.palantir.spark.shuffle.async.s3.util.SizedInput;
import com.palantir.spark.shuffle.async.s3.util.streams.SeekableInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;

public final class MergingHadoopShuffleClient implements ShuffleClient {

    private final String appId;
    private final ListeningExecutorService downloadExecService;
    private final ListeningExecutorService localReadExecService;
    private final MergingShuffleClientMetrics metrics;
    private final ShuffleDriverEndpointRef shuffleDriverEndpointRef;
    private final MergingShuffleUploadCoordinator shuffleUploadCoordinator;
    private final MergingShuffleFiles shuffleFiles;
    private final Map<Long, ListenableFuture<?>> downloadDataTasks;
    private final Map<Long, ListenableFuture<?>> downloadIndicesTasks;
    private final Clock clock;

    public MergingHadoopShuffleClient(
            String appId,
            ListeningExecutorService downloadExecService,
            ListeningExecutorService localReadExecService,
            MergingShuffleClientMetrics metrics,
            ShuffleDriverEndpointRef shuffleDriverEndpointRef,
            MergingShuffleUploadCoordinator shuffleUploadCoordinator,
            MergingShuffleFiles shuffleFiles,
            Clock clock) {
        this.appId = appId;
        this.downloadExecService = downloadExecService;
        this.localReadExecService = localReadExecService;
        this.metrics = metrics;
        this.shuffleDriverEndpointRef = shuffleDriverEndpointRef;
        this.shuffleUploadCoordinator = shuffleUploadCoordinator;
        this.shuffleFiles = shuffleFiles;
        this.downloadDataTasks = new ConcurrentHashMap<>();
        this.downloadIndicesTasks = new ConcurrentHashMap<>();
        this.clock = clock;
    }

    @Override
    public void asyncWriteIndexFileAndClose(
            SizedInput indexSizedInput, int shuffleId, int mapId, long attemptId) {
        asyncWriteDataAndIndexFilesAndClose(
                new EmptySizedInput(),
                indexSizedInput,
                shuffleId,
                mapId,
                attemptId);
    }

    @Override
    public void asyncWriteDataAndIndexFilesAndClose(
            SizedInput dataSizedInput,
            SizedInput indexSizedInput,
            int shuffleId,
            int mapId,
            long attemptId) {
        shuffleUploadCoordinator.addShuffleMapInputForUpload(
                new ShuffleMapInput(
                        new MapOutputId(shuffleId, mapId, attemptId),
                        dataSizedInput,
                        indexSizedInput));
    }

    @Override
    public ListenableFuture<InputStream> getBlockData(
            int shuffleId, int mapId, int reduceId, long attemptId) {
        if (!shuffleFiles.doLocalBackupsExist(shuffleId, mapId, attemptId)) {
            Optional<Long> mergeId = shuffleDriverEndpointRef.getMapOutputMergeId(
                    new MapOutputId(shuffleId, mapId, attemptId));
            if (!mergeId.isPresent()) {
                throw new SafeIllegalStateException(
                        "Merge id was not generated for this block, so it must not have uploaded in time.",
                        SafeArg.of("appId", appId),
                        SafeArg.of("shuffleId", shuffleId),
                        SafeArg.of("mapId", mapId),
                        SafeArg.of("reduceId", reduceId),
                        SafeArg.of("attemptId", attemptId));
            }
            List<ListenableFuture<?>> downloadFutures = new ArrayList<>(2);
            if (!shuffleFiles.doesLocalBackupDataFileExist(shuffleId, mapId, attemptId)) {
                downloadFutures.add(startDownloadTask(
                        mergeId.get(),
                        downloadDataTasks,
                        () -> downloadMergedData(shuffleId, mapId, attemptId, mergeId.get())));
            }

            if (!shuffleFiles.doesLocalBackupIndexFileExist(shuffleId, mapId, attemptId)) {
                downloadFutures.add(startDownloadTask(
                        mergeId.get(),
                        downloadIndicesTasks,
                        () -> downloadMergedIndices(shuffleId, mapId, attemptId, mergeId.get())));
            }

            if (!downloadFutures.isEmpty()) {
                return FluentFuture.from(Futures.allAsList(downloadFutures))
                        .transform(
                                ignored -> getDownloadedBlockDataChecked(
                                        shuffleId, mapId, reduceId, attemptId),
                                localReadExecService);
            } else {
                return asyncGetDownloadedBlockDataChecked(shuffleId, mapId, reduceId, attemptId);
            }
        } else {
            return asyncGetDownloadedBlockDataChecked(shuffleId, mapId, reduceId, attemptId);
        }
    }

    private void downloadMergedData(int shuffleId, int mapId, long attemptId, long mergeId) {
        // Have to double check that something hasn't already done the download
        // TODO(mcheah): #
        if (!shuffleFiles.doesLocalBackupDataFileExist(shuffleId, mapId, attemptId)) {
            metrics.markDownloadRequested(shuffleId, mapId, attemptId, mergeId, "data");
            long startTimeMillis = clock.millis();
            try (InputStream mergedInput = shuffleFiles.openRemoteMergedDataFile(mergeId);
                    DataInputStream mergedDataInput = new DataInputStream(mergedInput)) {
                metrics.markDownloadStarted(shuffleId, mapId, attemptId, mergeId, "data");
                FileMerger.fetchAndSplitMergedInput(
                        mergedDataInput,
                        (splitShuffleId, splitMapId, splitAttemptId) -> {
                            if (!shuffleFiles.doesLocalBackupDataFileExist(
                                    splitShuffleId, splitMapId, splitAttemptId)) {
                                return Optional.of(() -> shuffleFiles.createLocalBackupDataFile(
                                        splitShuffleId, splitMapId, splitAttemptId));
                            } else {
                                return Optional.empty();
                            }
                        });
            } catch (IOException e) {
                metrics.markDownloadFailed(shuffleId, mapId, attemptId, mergeId, "data");
                throw new RuntimeException(e);
            }
            metrics.markDownloadCompleted(shuffleId, mapId, attemptId, mergeId, "data",
                    clock.millis() - startTimeMillis);
        }
    }

    private void downloadMergedIndices(int shuffleId, int mapId, long attemptId, long mergeId) {
        // Have to double check that something hasn't already done the download
        if (!shuffleFiles.doesLocalBackupIndexFileExist(shuffleId, mapId, attemptId)) {
            metrics.markDownloadRequested(shuffleId, mapId, attemptId, mergeId, "index");
            long startTimeMillis = clock.millis();
            try (InputStream mergedInput = shuffleFiles.openRemoteMergedIndexFile(mergeId);
                    DataInputStream mergedDataInput = new DataInputStream(mergedInput)) {
                metrics.markDownloadStarted(shuffleId, mapId, attemptId, mergeId, "index");
                FileMerger.fetchAndSplitMergedInput(
                        mergedDataInput,
                        (splitShuffleId, splitMapId, splitAttemptId) -> {
                            if (!shuffleFiles.doesLocalBackupIndexFileExist(
                                    splitShuffleId, splitMapId, splitAttemptId)) {
                                return Optional.of(() -> shuffleFiles.createLocalBackupIndexFile(
                                        splitShuffleId, splitMapId, splitAttemptId));
                            } else {
                                return Optional.empty();
                            }
                        });
            } catch (IOException e) {
                metrics.markDownloadFailed(shuffleId, mapId, attemptId, mergeId, "index");
                throw new RuntimeException(e);
            }
            metrics.markDownloadCompleted(shuffleId, mapId, attemptId, mergeId, "index",
                    clock.millis() - startTimeMillis);
        }
    }

    private ListenableFuture<?> startDownloadTask(
            long mergeId,
            Map<Long, ListenableFuture<?>> tasks,
            Runnable taskRunnable) {
        return tasks.computeIfAbsent(
                mergeId,
                resolvedMergeId -> {
                    ListenableFuture<?> downloadTask = downloadExecService.submit(taskRunnable);
                    downloadTask.addListener(
                            () -> tasks.remove(resolvedMergeId), downloadExecService);
                    return downloadTask;
                });
    }

    private ListenableFuture<InputStream> asyncGetDownloadedBlockDataChecked(
            int shuffleId,
            int mapId,
            int reduceId,
            long attemptId) {
        return localReadExecService.submit(
                () -> getDownloadedBlockDataChecked(
                        shuffleId, mapId, reduceId, attemptId));
    }

    private InputStream getDownloadedBlockDataChecked(
            int shuffleId,
            int mapId,
            int reduceId,
            long attemptId) {
        Preconditions.checkState(
                shuffleFiles.doLocalBackupsExist(shuffleId, mapId, attemptId),
                "Shuffle backup files should have been downloaded.",
                SafeArg.of("appId", appId),
                SafeArg.of("shuffleId", shuffleId),
                SafeArg.of("mapId", mapId),
                SafeArg.of("attemptId", attemptId));
        SeekableInput dataInput = shuffleFiles.getLocalBackupDataFile(shuffleId, mapId, attemptId);
        SeekableInput indexInput = shuffleFiles.getLocalBackupIndexFile(shuffleId, mapId, attemptId);
        return PartitionDecoder.decodePartition(dataInput, indexInput, reduceId);
    }

    @Override
    public void removeApplicationData() {

    }
}
