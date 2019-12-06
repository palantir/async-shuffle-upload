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

package com.palantir.spark.shuffle.async.s3.client.basic;

import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.palantir.logsafe.SafeArg;
import com.palantir.spark.shuffle.async.immutables.ImmutablesStyle;
import com.palantir.spark.shuffle.async.s3.client.ShuffleClient;
import com.palantir.spark.shuffle.async.s3.io.PartitionDecoder;
import com.palantir.spark.shuffle.async.s3.metadata.MapOutputId;
import com.palantir.spark.shuffle.async.s3.metrics.BasicShuffleClientMetrics;
import com.palantir.spark.shuffle.async.s3.util.SizedInput;
import com.palantir.spark.shuffle.async.s3.util.streams.SeekableHadoopInput;
import com.palantir.spark.shuffle.async.s3.util.streams.SeekableInput;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HadoopShuffleClient implements ShuffleClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopShuffleClient.class);

    private static final String DATA_PREFIX = "data";

    private final ListeningExecutorService downloadExecService;
    private final ListeningExecutorService uploadExecService;
    private final String appId;
    private final FileSystem fileSystem;
    private final URI baseUri;
    private final ShuffleDriverEndpointRef shuffleDriverEndpointRef;
    private final Clock clock;
    private final BasicShuffleClientMetrics metrics;
    private final int remoteFileBufferSize;
    private final AtomicLong pendingOrRunningUploadsCounter = new AtomicLong(0L);

    public HadoopShuffleClient(
            URI baseUri,
            String appId,
            ListeningExecutorService uploadExecService,
            ListeningExecutorService downloadExecService,
            FileSystem fileSystem,
            BasicShuffleClientMetrics metrics,
            ShuffleDriverEndpointRef shuffleDriverEndpointRef,
            int remoteFileBufferSize,
            Clock clock) {
        this.baseUri = baseUri;
        this.appId = appId;
        this.uploadExecService = uploadExecService;
        this.downloadExecService = downloadExecService;
        this.fileSystem = fileSystem;
        this.shuffleDriverEndpointRef = shuffleDriverEndpointRef;
        this.clock = clock;
        this.remoteFileBufferSize = remoteFileBufferSize;
        this.metrics = metrics;
    }

    @Override
    public void asyncWriteIndexFileAndClose(
            SizedInput indexSizedInput, int shuffleId, int mapId, long attemptId) {
        doSubmitUpload(
                Optional.empty(),
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
        doSubmitUpload(
                Optional.of(dataSizedInput),
                indexSizedInput,
                shuffleId,
                mapId,
                attemptId);
    }

    private void doSubmitUpload(
            Optional<SizedInput> dataSizedInput,
            SizedInput indexSizedInput,
            int shuffleId,
            int mapId,
            long attemptId) {
        long timeSubmitted = clock.millis();
        metrics.markUploadRequested(
                shuffleId,
                mapId,
                attemptId,
                pendingOrRunningUploadsCounter.incrementAndGet());
        ListenableFuture<Optional<ShuffleUploadTaskMetrics>> uploadTask = uploadExecService.submit(() ->
                doWriteFilesAndClose(
                        dataSizedInput,
                        indexSizedInput,
                        shuffleId,
                        mapId,
                        attemptId,
                        timeSubmitted));
        Futures.addCallback(
                uploadTask,
                new ShuffleUploadFinishedListener(
                        metrics,
                        pendingOrRunningUploadsCounter,
                        shuffleId,
                        mapId,
                        attemptId),
                uploadExecService);
        metrics.markUploadRequestSubmitted(
                shuffleId,
                mapId,
                attemptId,
                clock.millis() - timeSubmitted);
    }

    private void uploadFile(
            SizedInput sizedInput,
            Path hadoopFilePath,
            int shuffleId,
            int mapId,
            long attemptId) {
        try (InputStream openInputStream = sizedInput.openStream();
                OutputStream dataOutput = createRemoteFsOutputStream(hadoopFilePath)) {
            ByteStreams.copy(openInputStream, dataOutput);
        } catch (IOException e) {
            LOGGER.error(
                    "Exception encountered while uploading data file.",
                    SafeArg.of("appId", appId),
                    SafeArg.of("shuffleId", shuffleId),
                    SafeArg.of("mapId", mapId),
                    SafeArg.of("attemptId", attemptId),
                    e);

            throw new RuntimeException(e);
        }
    }

    private Optional<ShuffleUploadTaskMetrics> doWriteFilesAndClose(
            Optional<SizedInput> dataSizedInput,
            SizedInput indexSizedInput,
            int shuffleId,
            int mapId,
            long attemptId,
            long timeSubmitted) {
        long startTime = clock.millis();
        try {
            if (!shuffleDriverEndpointRef.isShuffleRegistered(shuffleId)) {
                LOGGER.info(
                        "Shuffle is no longer active; skipping file upload",
                        SafeArg.of("appId", appId),
                        SafeArg.of("shuffleId", shuffleId),
                        SafeArg.of("mapId", mapId),
                        SafeArg.of("attemptId", attemptId));
                return Optional.empty();
            }
        } catch (RuntimeException e) {
            LOGGER.error("Exception encountered while checking for existence of shuffle.",
                    SafeArg.of("appId", appId),
                    SafeArg.of("shuffleId", shuffleId),
                    SafeArg.of("mapId", mapId),
                    SafeArg.of("attemptId", attemptId),
                    e);
            throw new RuntimeException(e);
        }

        metrics.markUploadStarted(shuffleId, mapId, attemptId);
        dataSizedInput.ifPresent(
                input -> uploadFile(
                        input,
                        toDataFile(shuffleId, mapId, attemptId),
                        shuffleId,
                        mapId,
                        attemptId));

        uploadFile(indexSizedInput, toIndexFile(shuffleId, mapId, attemptId), shuffleId, mapId, attemptId);
        shuffleDriverEndpointRef.registerUnmergedMapOutput(MapOutputId.apply(shuffleId, mapId, attemptId));
        long totalSize = dataSizedInput
                .map(input -> indexSizedInput.getStreamSizeInBytes() + input.getStreamSizeInBytes())
                .orElseGet(() -> indexSizedInput.getStreamSizeInBytes());
        long now = clock.millis();
        return Optional.of(
                ImmutableShuffleUploadTaskMetrics.builder()
                        .totalBytesWritten(totalSize)
                        .uploadDurationMillis(now - startTime)
                        .uploadLatencyMillis(now - timeSubmitted)
                        .build());
    }

    @Override
    public void removeApplicationData() {
        try {
            fileSystem.delete(new Path(baseUri.toString(), String.format("%s/%s", DATA_PREFIX, appId)), true);
        } catch (IOException e) {
            LOGGER.error("Encountered error deleting application data",
                    SafeArg.of("appId", appId),
                    e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ListenableFuture<InputStream> getBlockData(
            int shuffleId,
            int mapId,
            int reduceId,
            long attemptId) {
        return downloadExecService.submit(() -> {
            metrics.markDownloadRequested(shuffleId, mapId, reduceId, attemptId);
            SeekableInput dataInput = new SeekableHadoopInput(toDataFile(shuffleId, mapId, attemptId), fileSystem);
            SeekableInput indexInput = new SeekableHadoopInput(toIndexFile(shuffleId, mapId, attemptId), fileSystem);
            InputStream resolvedInputStream = PartitionDecoder.decodePartition(dataInput, indexInput, reduceId);
            metrics.markDownloadStarted(shuffleId, mapId, reduceId, attemptId);
            return resolvedInputStream;
        });
    }

    private OutputStream createRemoteFsOutputStream(Path path) throws IOException {
        return new BufferedOutputStream(fileSystem.create(path), remoteFileBufferSize);
    }

    private Path toDataFile(int shuffleId, int mapId, long attemptId) {
        return new Path(baseUri.toString(),
                String.format("%s/%s/%d/%d/%d.data", DATA_PREFIX, appId, shuffleId, mapId, attemptId));
    }

    private Path toIndexFile(int shuffleId, int mapId, long attemptId) {
        return new Path(baseUri.toString(),
                String.format("%s/%s/%d/%d/%d.index", DATA_PREFIX, appId, shuffleId, mapId, attemptId));
    }

    @Value.Immutable
    @ImmutablesStyle
    interface ShuffleUploadTaskMetrics {

        long totalBytesWritten();

        long uploadDurationMillis();

        long uploadLatencyMillis();
    }

    private static final class ShuffleUploadFinishedListener
            implements FutureCallback<Optional<ShuffleUploadTaskMetrics>> {

        private final BasicShuffleClientMetrics metrics;
        private final AtomicLong pendingOrRunningUploadsCounter;
        private final int shuffleId;
        private final int mapId;
        private final long attemptId;

        ShuffleUploadFinishedListener(
                BasicShuffleClientMetrics metrics,
                AtomicLong pendingOrRunningUploadsCounter,
                int shuffleId,
                int mapId,
                long attemptId) {
            this.metrics = metrics;
            this.pendingOrRunningUploadsCounter = pendingOrRunningUploadsCounter;
            this.shuffleId = shuffleId;
            this.mapId = mapId;
            this.attemptId = attemptId;
        }

        @Override
        public void onSuccess(Optional<ShuffleUploadTaskMetrics> maybeUploadTaskMetrics) {
            maybeUploadTaskMetrics.ifPresent(uploadTaskMetrics ->
                    this.metrics.markUploadCompleted(
                            shuffleId,
                            mapId,
                            attemptId,
                            uploadTaskMetrics.uploadDurationMillis(),
                            uploadTaskMetrics.totalBytesWritten(),
                            uploadTaskMetrics.uploadLatencyMillis(),
                            pendingOrRunningUploadsCounter.decrementAndGet()));
        }

        @Override
        public void onFailure(Throwable error) {
            metrics.markUploadFailed(shuffleId, mapId, attemptId, pendingOrRunningUploadsCounter.decrementAndGet());
            LOGGER.error("Failed to upload shuffle files.", error);
        }
    }
}
