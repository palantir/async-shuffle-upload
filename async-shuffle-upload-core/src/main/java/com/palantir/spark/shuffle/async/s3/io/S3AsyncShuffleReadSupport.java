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

import com.palantir.spark.shuffle.async.s3.client.ShuffleClient;
import com.palantir.spark.shuffle.async.s3.metrics.S3FetcherIteratorMetrics;
import com.palantir.spark.shuffle.async.s3.reader.DefaultS3FetcherIterator;
import com.palantir.spark.shuffle.async.s3.reader.FallbackToS3ShuffleIterator;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.apache.spark.TaskContext;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.api.ShuffleBlockInfo;
import org.apache.spark.shuffle.api.ShuffleBlockInputStream;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.util.TaskCompletionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class S3AsyncShuffleReadSupport {

    private static final Logger LOG = LoggerFactory.getLogger(S3AsyncShuffleReadSupport.class);

    private final ShuffleExecutorComponents delegate;
    private final ShuffleClient client;
    private final SerializerManager serializerManager;
    private final CompressionCodec compressionCodec;
    private final boolean shouldCompressShuffle;
    private final ExecutorService enqueuerExecutorService;
    private final ScheduledExecutorService timeoutExecutorService;
    private final S3FetcherIteratorMetrics metrics;
    private final Supplier<Optional<TaskContext>> taskContext;
    private final ShuffleDriverEndpointRef driverEndpointRef;

    public S3AsyncShuffleReadSupport(
            ShuffleExecutorComponents delegate,
            ShuffleClient client,
            SerializerManager serializerManager,
            CompressionCodec compressionCodec,
            boolean shouldCompressShuffle,
            ExecutorService enqueuerExecutorService,
            ScheduledExecutorService timeoutExecutorService,
            S3FetcherIteratorMetrics metrics,
            Supplier<Optional<TaskContext>> taskContext,
            ShuffleDriverEndpointRef driverEndpointRef) {
        this.delegate = delegate;
        this.client = client;
        this.serializerManager = serializerManager;
        this.compressionCodec = compressionCodec;
        this.shouldCompressShuffle = shouldCompressShuffle;
        this.enqueuerExecutorService = enqueuerExecutorService;
        this.timeoutExecutorService = timeoutExecutorService;
        this.metrics = metrics;
        this.taskContext = taskContext;
        this.driverEndpointRef = driverEndpointRef;
    }

    public Iterable<ShuffleBlockInputStream> getPartitionReaders(
            Iterable<ShuffleBlockInfo> blockMetadata) throws IOException {
        LOG.debug("Creating s3 shuffle partition reader");
        final Set<ShuffleBlockInfo> shuffleBlocksFromExecutors = new HashSet<>();
        final Set<ShuffleBlockInfo> shuffleBlocksFromRemote = new HashSet<>();
        Set<BlockManagerId> blacklistedExecutors = driverEndpointRef.getBlacklistedExecutors();
        blockMetadata.forEach(blockInfo -> {
            BlockManagerId blockManagerId = blockInfo.getShuffleLocation().get();
            if (blacklistedExecutors.contains(blockManagerId)) {
                shuffleBlocksFromRemote.add(blockInfo);
            } else {
                shuffleBlocksFromExecutors.add(blockInfo);
            }
        });

        Iterable<ShuffleBlockInputStream> inputStreams = delegate.getPartitionReaders(shuffleBlocksFromExecutors);
        return () -> {
            FallbackToS3ShuffleIterator iterator = new FallbackToS3ShuffleIterator(
                    inputStreams.iterator(),
                    shuffleBlocksFromExecutors,
                    shouldCompressShuffle,
                    serializerManager,
                    compressionCodec,
                    shuffleBlocksFromRemote,
                    new DefaultS3FetcherIterator(client, enqueuerExecutorService, timeoutExecutorService, metrics),
                    driverEndpointRef);
            taskContext.get().ifPresent(context ->
                    context.addTaskCompletionListener(new FallbackToS3ShuffleCompletionIterator(iterator)));
            return iterator;
        };
    }

    private static final class FallbackToS3ShuffleCompletionIterator implements TaskCompletionListener {

        private final FallbackToS3ShuffleIterator fallbackToS3Iterator;

        FallbackToS3ShuffleCompletionIterator(FallbackToS3ShuffleIterator fallbackToS3Iterator) {
            this.fallbackToS3Iterator = fallbackToS3Iterator;
        }

        @Override
        @SuppressWarnings("StrictUnusedVariable")
        public void onTaskCompletion(TaskContext context) {
            this.fallbackToS3Iterator.cleanup();
        }
    }
}
