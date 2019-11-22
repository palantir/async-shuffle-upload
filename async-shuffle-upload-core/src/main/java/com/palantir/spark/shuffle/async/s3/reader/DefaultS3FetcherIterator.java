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

package com.palantir.spark.shuffle.async.s3.reader;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.spark.shuffle.async.immutables.ImmutablesStyle;
import com.palantir.spark.shuffle.async.s3.client.ShuffleClient;
import com.palantir.spark.shuffle.async.s3.metrics.S3FetcherIteratorMetrics;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.palantir.shuffle.async.FetchFailedExceptionThrower;
import org.apache.spark.shuffle.api.ShuffleBlockInfo;
import org.apache.spark.shuffle.api.ShuffleBlockInputStream;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.ShuffleBlockId;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DefaultS3FetcherIterator implements S3FetcherIterator {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultS3FetcherIterator.class);

    private static final Duration TIMEOUT = Duration.ofMinutes(5);

    private final BlockingQueue<BlockDataResult> fetchResults;
    private final ShuffleClient client;
    private final AtomicInteger streamCount;
    private final ExecutorService executorService;
    private final ScheduledExecutorService timeoutExecutorService;
    private final S3FetcherIteratorMetrics metrics;

    private boolean initialized;

    private final Map<ShuffleBlockId, Future<InputStream>> activeFetchRequests;

    public DefaultS3FetcherIterator(
            ShuffleClient client,
            ExecutorService executorService,
            ScheduledExecutorService timeoutExecutorService,
            S3FetcherIteratorMetrics metrics) {
        this.fetchResults = Queues.newLinkedBlockingQueue();
        this.activeFetchRequests = new ConcurrentHashMap<>();
        this.client = client;
        this.streamCount = new AtomicInteger();
        this.executorService = executorService;
        this.timeoutExecutorService = timeoutExecutorService;
        this.metrics = metrics;
    }

    @Override
    public void initialize(Collection<ShuffleBlockInfo> blocksToFetch) {
        if (initialized) {
            throw new SafeRuntimeException("DefaultS3FetcherIterator can only be initialized once");
        }
        initialized = true;
        fetchDataFromS3(blocksToFetch);
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    private void fetchDataFromS3(Collection<ShuffleBlockInfo> blocksToFetch) {
        // Find all blocks that were not yet successfully fetched from the executors.
        blocksToFetch.forEach(shuffleBlockInfo -> {
            // Request all remaining blocks from remote storage.
            // Not necessarily 100% accurate - for example, there may be other executors
            // remaining where we could have fetched the data from. But:

            // 1. The lifecycle of using the underlying iterator that fetches from other
            //    executors, is more or less unpredictable after a fetch failure. And,
            // 2. It's better to go to remote storage excessively, than attempting to fetch blocks
            //    from any executors that might be down at all - since attempting to fetch from
            //    dead executors requires waiting for a costly timeout
            fetchBlock(shuffleBlockInfo);
            metrics.markFetchFromExecutorFailed(
                    shuffleBlockInfo.getShuffleId(),
                    shuffleBlockInfo.getMapId(),
                    shuffleBlockInfo.getReduceId(),
                    shuffleBlockInfo.getMapTaskAttemptId());
        });
    }

    private void fetchBlock(ShuffleBlockInfo shuffleBlockInfo) {
        int shuffleId = shuffleBlockInfo.getShuffleId();
        int mapId = shuffleBlockInfo.getMapId();
        int reduceId = shuffleBlockInfo.getReduceId();
        long attemptId = shuffleBlockInfo.getMapTaskAttemptId();

        streamCount.incrementAndGet();
        ShuffleBlockId shuffleBlockId = ShuffleBlockId.apply(shuffleId, mapId, reduceId);
        FluentFuture<InputStream> fetchFuture =
                FluentFuture.from(client.getBlockData(shuffleId, mapId, reduceId, attemptId));
        activeFetchRequests.put(shuffleBlockId, fetchFuture);
        fetchFuture
                .withTimeout(TIMEOUT.getSeconds(), TimeUnit.SECONDS, timeoutExecutorService)
                .<BlockDataResult>transform(
                        stream -> {
                            LOG.info("Successfully opened input stream for map output from remote storage.",
                                    SafeArg.of("shuffleId", shuffleId),
                                    SafeArg.of("mapId", mapId),
                                    SafeArg.of("reduceId", reduceId),
                                    SafeArg.of("attemptId", attemptId));
                            metrics.markFetchFromRemoteSucceeded(shuffleId, mapId, reduceId, attemptId);
                            return BlockDataSuccessResult.of(
                                    new ShuffleBlockInputStream(
                                            shuffleBlockId,
                                            stream));
                        }, executorService)

                .catching(
                        Exception.class,
                        error -> {
                            LOG.error("Failed to get stream for map output from remote storage.",
                                    SafeArg.of("shuffleId", shuffleId),
                                    SafeArg.of("mapId", mapId),
                                    SafeArg.of("reduceId", reduceId),
                                    SafeArg.of("attemptId", attemptId),
                                    error);
                            metrics.markFetchFromRemoteFailed(shuffleId, mapId, reduceId, attemptId);
                            return BlockDataErrorResult.builder()
                                    .error(error)
                                    .shuffleBlockId(shuffleBlockId)
                                    .blockManagerId(shuffleBlockInfo.getShuffleLocation().get())
                                    .build();
                        },
                        executorService)
                .transform(fetchResults::add, executorService)
                .addCallback(new FutureCallback<Boolean>() {
                    @Override
                    public void onSuccess(Boolean _result) {
                        LOG.info("Successfully added block result to queue.");
                    }

                    @Override
                    public void onFailure(Throwable error) {
                        // Error is event-logged in the catching block above
                        LOG.error("Error occurred in pushing fetch result into the queue.", error);
                    }
                }, executorService);
    }

    @Override
    public void cleanup() {
        if (initialized) {
            int streamsRemaining = streamCount.get();
            // cancel all active things
            LOG.info("Cleaning up the DefaultS3FetcherIterator");
            AtomicInteger numFuturesCancelled = new AtomicInteger();
            activeFetchRequests.values().forEach(future -> {
                if (!future.isDone()) {
                    future.cancel(true);
                    numFuturesCancelled.addAndGet(1);
                }
            });

            // Close all input streams that haven't yet been retrieved.
            int streamsToClose = streamsRemaining - numFuturesCancelled.get();
            for (int i = 0; i < streamsToClose; i++) {
                try {
                    BlockDataResult blockDataResult = fetchResults.take();
                    blockDataResult.close();
                } catch (InterruptedException e) {
                    LOG.warn("Encountered exception while trying to get head of the results queue.", e);
                } catch (IOException e) {
                    LOG.warn("Encountered exception while trying to close stream", e);
                }
            }
            LOG.info(
                    "Cleaned up the DefaultS3FetcherIterator",
                    SafeArg.of("numFuturesCancelled", numFuturesCancelled),
                    SafeArg.of("numStreamsClosed", streamsToClose));
        }
    }

    @Override
    public boolean hasNext() {
        checkInitialized();
        return streamCount.get() > 0;
    }

    @Override
    public ShuffleBlockInputStream next() {
        checkInitialized();
        if (streamCount.getAndDecrement() > 0) {
            try {
                BlockDataResult result = fetchResults.poll(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
                activeFetchRequests.remove(result.getShuffleBlockId());
                return result.getResult();
            } catch (InterruptedException e) {
                LOG.error("Error encountered while waiting for InputStream to become available", e);
                throw new SafeRuntimeException(e);
            }
        }
        throw new SafeRuntimeException("Next should not be called because iterator is empty");
    }

    private void checkInitialized() {
        Preconditions.checkState(initialized, "S3 Fetcher iterator is not initialized");
    }

    interface BlockDataResult {
        ShuffleBlockId getShuffleBlockId();
        ShuffleBlockInputStream getResult();
        default void close() throws IOException {}
    }

    @Value.Immutable
    @ImmutablesStyle
    abstract static class BlockDataSuccessResult implements BlockDataResult {
        abstract ShuffleBlockInputStream blockDataStream();

        @Override
        public ShuffleBlockId getShuffleBlockId() {
            return toShuffleBlockId(blockDataStream().getBlockId());
        }

        @Override
        public final ShuffleBlockInputStream getResult() {
            return blockDataStream();
        }

        @Override
        public void close() throws IOException {
            blockDataStream().close();
        }

        static BlockDataSuccessResult of(ShuffleBlockInputStream blockDataStream) {
            return builder().blockDataStream(blockDataStream).build();
        }

        static Builder builder() {
            return new Builder();
        }

        static final class Builder extends ImmutableBlockDataSuccessResult.Builder {}
    }

    @Value.Immutable
    @ImmutablesStyle
    abstract static class BlockDataErrorResult implements BlockDataResult {
        abstract Exception error();

        abstract ShuffleBlockId shuffleBlockId();

        abstract BlockManagerId blockManagerId();

        @Override
        public ShuffleBlockId getShuffleBlockId() {
            return shuffleBlockId();
        }

        @Override
        public final ShuffleBlockInputStream getResult() {
            // The below actually throws an error - but since FetchFailedException is a checked
            // exception, run Scala code to throw this exception and bypass the need to declare
            // throws or catch and wrap with RuntimeException. We have to throw exactly a
            // FetchFailedException and cannot wrap that exception in anything else.
            return FetchFailedExceptionThrower.throwFetchFailedException(
                    shuffleBlockId().shuffleId(),
                    shuffleBlockId().mapId(),
                    shuffleBlockId().reduceId(),
                    blockManagerId(),
                    "Exception thrown when fetching data from remote storage.",
                    error());
        }

        static Builder builder() {
            return new Builder();
        }

        static final class Builder extends ImmutableBlockDataErrorResult.Builder {}
    }

    private static ShuffleBlockId toShuffleBlockId(BlockId blockId) {
        if (blockId instanceof ShuffleBlockId) {
            return (ShuffleBlockId) blockId;
        }
        throw new SafeRuntimeException("Expected block id to be of instance ShuffleBLockAttemptId");
    }
}
