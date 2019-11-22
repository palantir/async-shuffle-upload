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

package com.palantir.spark.shuffle.async.s3.io;

import com.google.common.collect.Queues;
import com.google.common.io.CountingOutputStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.exceptions.SafeIoException;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.palantir.shuffle.async.FetchFailedExceptionThrower;
import org.apache.spark.shuffle.api.MapOutputWriterCommitMessage;
import org.apache.spark.shuffle.api.ShuffleBlockInfo;
import org.apache.spark.shuffle.api.ShuffleBlockInputStream;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.ShuffleBlockId;

/**
 * Mirrors the default shuffle executor components but strips away many of the Spark-specific
 * components - particularly the IndexShuffleBlockResolver.
 */
public final class TestShuffleExecutorComponents implements ShuffleExecutorComponents {

    private static final int BLOCK_MANAGER_ID_PORT = 7077;
    private static final BlockManagerId BLOCK_MANAGER_ID =
            BlockManagerId.apply("localhost", BLOCK_MANAGER_ID_PORT);

    private final ShuffleFileLocator shuffleFileLocator;

    public TestShuffleExecutorComponents(ShuffleFileLocator shuffleFileLocator) {
        this.shuffleFileLocator = shuffleFileLocator;
    }

    @Override
    public void initializeExecutor(
            String _appId, String _execId, Map<String, String> _extraConfigs) {}

    @Override
    public ShuffleMapOutputWriter createMapOutputWriter(
            int shuffleId, int mapId, long _mapTaskAttemptId, int _numPartitions) throws IOException {
        return new TestShuffleMapOutputWriter(shuffleFileLocator, shuffleId, mapId);
    }

    @Override
    public Iterable<ShuffleBlockInputStream> getPartitionReaders(Iterable<ShuffleBlockInfo> blockMetadata)
            throws IOException {
        return () -> {
            Iterator<ShuffleBlockInfo> blockMetadataIt = blockMetadata.iterator();
            return new Iterator<ShuffleBlockInputStream>() {

                @Override
                public boolean hasNext() {
                    return blockMetadataIt.hasNext();
                }

                @Override
                public ShuffleBlockInputStream next() {
                    ShuffleBlockInfo block = blockMetadataIt.next();
                    File dataFile = shuffleFileLocator.getDataFile(block.getShuffleId(), block.getMapId());
                    File indexFile = shuffleFileLocator.getIndexFile(
                            block.getShuffleId(),
                            block.getMapId());
                    if (!dataFile.isFile() || !indexFile.isFile()) {
                        FetchFailedExceptionThrower.throwFetchFailedException(
                                block.getShuffleId(),
                                block.getMapId(),
                                block.getReduceId(),
                                BLOCK_MANAGER_ID,
                                "Test fetch failed.",
                                null);
                    }
                    long dataOffset;
                    long nextOffset;

                    try (SeekableByteChannel indexFileInput = Files.newByteChannel(indexFile.toPath())) {
                        indexFileInput.position(block.getReduceId() * 8L);
                        DataInputStream indexDataIn =
                                new DataInputStream(Channels.newInputStream(indexFileInput));
                        dataOffset = indexDataIn.readLong();
                        nextOffset = indexDataIn.readLong();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    try {
                        SeekableByteChannel dataIn = Files.newByteChannel(dataFile.toPath());
                        dataIn.position(dataOffset);
                        return new ShuffleBlockInputStream(
                                new ShuffleBlockId(block.getShuffleId(), block.getMapId(), block.getReduceId()),
                                new LimitedInputStream(Channels.newInputStream(dataIn), nextOffset - dataOffset));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        };
    }

    private static final class TestShuffleMapOutputWriter implements ShuffleMapOutputWriter {
        private final ShuffleFileLocator shuffleFileLocator;
        private final int shuffleId;
        private final int mapId;
        private final ByteArrayOutputStream partitionBytesOut;
        private final Queue<Long> partitionLengths;

        TestShuffleMapOutputWriter(
                ShuffleFileLocator shuffleFileLocator,
                int shuffleId,
                int mapId) {
            this.shuffleFileLocator = shuffleFileLocator;
            this.shuffleId = shuffleId;
            this.mapId = mapId;
            this.partitionBytesOut = new ByteArrayOutputStream();
            this.partitionLengths = Queues.newArrayDeque();
        }

        @Override
        public ShufflePartitionWriter getPartitionWriter(int _partitionId) {
            return new TestShufflePartitionWriter(partitionBytesOut, partitionLengths);
        }

        @Override
        public MapOutputWriterCommitMessage commitAllPartitions() throws IOException {
            File dataFile = shuffleFileLocator.getDataFile(shuffleId, mapId);
            File indexFile = shuffleFileLocator.getIndexFile(shuffleId, mapId);
            if (dataFile.exists()) {
                throw new SafeIllegalStateException(
                        "Data file already exists.", SafeArg.of("dataFile", dataFile.getAbsolutePath()));
            }
            if (!dataFile.getParentFile().isDirectory() && !dataFile.getParentFile().mkdirs()) {
                throw new SafeIoException(
                        "Failed to create the data directory.",
                        SafeArg.of("dataDir", dataFile.getParentFile().getAbsolutePath()));
            }
            Files.write(dataFile.toPath(), partitionBytesOut.toByteArray());
            try (DataOutputStream indexOut = new DataOutputStream(new FileOutputStream(indexFile))) {
                // We take in lengths of each block, need to convert it to offsets.
                long offset = 0L;
                indexOut.writeLong(offset);
                for (long length : partitionLengths) {
                    offset += length;
                    indexOut.writeLong(offset);
                }
            }
            return MapOutputWriterCommitMessage.of(partitionLengths.stream().mapToLong(Long::valueOf).toArray());
        }

        @Override
        public void abort(Throwable _error) {}
    }

    private static final class TestShufflePartitionWriter implements ShufflePartitionWriter {

        private final CountingOutputStream countingPartitionBytesOut;
        private final Queue<Long> partitionLengths;

        TestShufflePartitionWriter(
                ByteArrayOutputStream partitionBytesOut, Queue<Long> partitionLengths) {
            this.countingPartitionBytesOut = new CountingOutputStream(partitionBytesOut);
            this.partitionLengths = partitionLengths;
        }

        @Override
        public OutputStream openStream() {
            return new FilterOutputStream(countingPartitionBytesOut) {
                @Override
                public void close() throws IOException {
                    flush();
                    partitionLengths.add(countingPartitionBytesOut.getCount());
                }
            };
        }

        @Override
        public long getNumBytesWritten() {
            return countingPartitionBytesOut.getCount();
        }
    }
}
