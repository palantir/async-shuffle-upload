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

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.logsafe.Preconditions;
import com.palantir.spark.shuffle.async.data.config.DataGeneratorConfig;
import com.palantir.spark.shuffle.async.s3.util.DaemonExecutors;
import com.palantir.spark.shuffle.async.s3.util.SizedInput;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AsyncShuffleBenchmark {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncShuffleBenchmark.class);

    private static final long MB = 1024 * 1024;
    private static final int SHUFFLE_ID = 10;
    private static final int ATTEMPT_ID = 0;
    private static final int READ_BUFFER_SIZE = 1024 * 1024;

    private final ShuffleClient client;
    private final ExecutorService uploadService;
    private final ExecutorService downloadService;
    private final List<Future<Boolean>> uploadFutures;
    private final List<Future<Boolean>> downloadFutures;

    private final long numBytesInPartition;
    private final int numPartitions;
    private final int numFiles;

    public AsyncShuffleBenchmark(DataGeneratorConfig config, ShuffleClient client) {
        this.client = client;
        this.uploadService = DaemonExecutors.newFixedDaemonThreadPool(config.writers());
        this.downloadService = DaemonExecutors.newFixedDaemonThreadPool(config.readers());
        this.numBytesInPartition = config.partitionSizeMb() * MB;
        this.numFiles = config.totalTestSizeMb() / config.shuffleSizeMb();
        this.numPartitions = config.shuffleSizeMb() / config.partitionSizeMb();
        this.uploadFutures = new ArrayList<>();
        this.downloadFutures = new ArrayList<>();
    }

    @SuppressWarnings("Slf4jConstantLogMessage")
    public double runWrite() throws ExecutionException, InterruptedException, IOException {
        LOGGER.info("Generating data to write");
        ByteArrayOutputStream byteArrayOs = new ByteArrayOutputStream();
        DataOutputStream indexOutput = new DataOutputStream(byteArrayOs);
        indexOutput.writeLong(0);
        for (int i = 0; i < numPartitions; i++) {
            indexOutput.writeLong((i + 1) * numBytesInPartition);
        }
        indexOutput.close();
        byte[] indexFileBytes = byteArrayOs.toByteArray();

        LOGGER.info("Beginning upload phase of benchmark");
        long startMillis = System.currentTimeMillis();
        byte[] oneMbBytes = RandomUtils.nextBytes((int) MB);

        for (int i = 0; i < numFiles; i++) {
            int mapId = i;
            uploadFutures.add(uploadService.submit(() -> {
                client.asyncWriteDataAndIndexFilesAndClose(
                        new ShuffleDataInputStream(numBytesInPartition * numPartitions, oneMbBytes),
                        new ByteArraySizedInput(indexFileBytes),
                        SHUFFLE_ID,
                        mapId,
                        ATTEMPT_ID);
                return true;
            }));
        }

        LOGGER.info("Triggered all upload jobs. Now reaping all the futures.");

        int count = 0;
        int totalJobs = uploadFutures.size();
        int stepSize = (int) Math.ceil((1.0 * totalJobs) / 10);
        for (Future<Boolean> future : uploadFutures) {
            boolean result = future.get();
            Preconditions.checkState(result, "Future did not successfully finish");
            count++;
            if (count % stepSize == 0) {
                String message = String.format("Reaped %s out of %s jobs", count, totalJobs);
                LOGGER.info(message);
            }
        }

        double totalBytes = numBytesInPartition * numFiles * numPartitions;
        double writeThrougputBytesPerSec =
                totalBytes / Duration.ofMillis(System.currentTimeMillis() - startMillis).getSeconds();

        return writeThrougputBytesPerSec;
    }

    @SuppressWarnings("Slf4jConstantLogMessage")
    public double runRead() throws ExecutionException, InterruptedException {
        LOGGER.info("Beginning download phase of benchmark");
        long startMillis = System.currentTimeMillis();

        for (int i = 0; i < numFiles; i++) {
            int mapId = i;
            for (int j = 0; j < numPartitions; j++) {
                int reduceId = j;
                downloadFutures.add(downloadService.submit(() -> {
                    ListenableFuture<InputStream> future = client.getBlockData(
                            SHUFFLE_ID,
                            mapId,
                            reduceId,
                            ATTEMPT_ID);
                    byte[] bytes = new byte[READ_BUFFER_SIZE];
                    try (InputStream stream = future.get()) {
                        while (stream.read(bytes) != -1) {
                            // do nothing
                        }
                        return true;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
        }

        LOGGER.info("Triggered all download jobs. Now reaping all the futures.");

        int count = 0;
        int totalJobs = downloadFutures.size();
        int stepSize = totalJobs / 10;
        for (Future<Boolean> future : downloadFutures) {
            boolean result = future.get();
            Preconditions.checkState(result, "Future did not successfully finish");
            count++;
            if (count % stepSize == 0) {
                String message = String.format("Reaped %s out of %s jobs", count, totalJobs);
                LOGGER.info(message);
            }
        }
        double totalBytes = numBytesInPartition * numFiles * numPartitions;
        double readThrougputBytesPerSec =
                totalBytes / Duration.ofMillis(System.currentTimeMillis() - startMillis).getSeconds();

        return readThrougputBytesPerSec;
    }

    private static final class ByteArraySizedInput implements SizedInput {
        private final byte[] bytes;

        ByteArraySizedInput(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public long getStreamSizeInBytes() {
            return bytes.length;
        }

        @Override
        public InputStream openStream() {
            return new ByteArrayInputStream(bytes);
        }
    }

    private static final class ShuffleDataInputStream extends InputStream implements SizedInput {

        private final byte[] repeatedBytes;
        private final int length;
        private final long totalStreamLength;

        private long remainingBytes;
        private int pos;

        ShuffleDataInputStream(long size, byte[] repeatedBytes) {
            this.repeatedBytes = repeatedBytes;
            this.remainingBytes = size;
            this.totalStreamLength = size;
            this.pos = 0;
            this.length = repeatedBytes.length;
        }

        @Override
        public int read() throws IOException {
            if (remainingBytes > 0) {
                remainingBytes--;
                int read = repeatedBytes[pos];
                pos = (pos + 1) % length;
                return read;
            }
            return -1;
        }

        @Override
        public int read(byte[] bytes, int off, int len) {
            if (remainingBytes <= 0) {
                return -1;
            }
            int bytesToRead = len;
            if (bytesToRead > remainingBytes) {
                bytesToRead = (int) remainingBytes;
            }

            remainingBytes -= bytesToRead;

            if (pos + bytesToRead <= length) {
                System.arraycopy(repeatedBytes, pos, bytes, off, bytesToRead);
                pos = (pos + bytesToRead) % length;
                return bytesToRead;
            }

            int bytesToReadFromEnd = length - pos;
            System.arraycopy(repeatedBytes, pos, bytes, off, bytesToReadFromEnd);
            int bytesToReadFromBeginning = bytesToRead - bytesToReadFromEnd;
            System.arraycopy(repeatedBytes, 0, bytes, off + bytesToReadFromEnd, bytesToReadFromBeginning);
            pos = (pos + bytesToRead) % length;
            return bytesToRead;
        }

        @Override
        public long getStreamSizeInBytes() {
            return totalStreamLength;
        }

        @Override
        public InputStream openStream() {
            return this;
        }
    }
}
