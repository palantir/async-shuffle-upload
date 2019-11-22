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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.palantir.logsafe.Preconditions;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.palantir.shuffle.async.FetchFailedExceptionThrower;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.FetchFailedException;
import org.apache.spark.shuffle.api.ShuffleBlockInfo;
import org.apache.spark.shuffle.api.ShuffleBlockInputStream;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.ShuffleBlockId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class FallbackToS3ShuffleIteratorTest {

    private static final BlockManagerId BLOCK_MANAGER_ID = BlockManagerId.apply("host", 1234);
    private static final ShuffleBlockId BLOCK_1 = ShuffleBlockId.apply(0, 1, 1);
    private static final ShuffleBlockInfo BLOCK_INFO_1 =
            new ShuffleBlockInfo(
                    BLOCK_1.shuffleId(), BLOCK_1.mapId(), BLOCK_1.reduceId(), 10, 1,
                    org.apache.spark.api.java.Optional.of(BLOCK_MANAGER_ID));

    private static final ShuffleBlockId BLOCK_2 = ShuffleBlockId.apply(0, 2, 1);
    private static final ShuffleBlockInfo BLOCK_INFO_2 =
            new ShuffleBlockInfo(
                    BLOCK_2.shuffleId(), BLOCK_2.mapId(), BLOCK_2.reduceId(), 10, 1,
                    org.apache.spark.api.java.Optional.of(BLOCK_MANAGER_ID));

    @Mock
    private SerializerManager serializerManager;
    @Mock
    private CompressionCodec compressionCodec;
    @Mock
    private ShuffleDriverEndpointRef driverEndpointRef;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testRetrievesFromExecutorOnly() {
        FallbackToS3ShuffleIterator iteratorUnderTest = getIteratorUnderTest(
                new FetchFailedThrowingStreamList().addStream(BLOCK_INFO_1),
                ImmutableSet.of(),
                ImmutableSet.of());
        assertThat(iteratorUnderTest.hasNext()).isTrue();
        assertThat(iteratorUnderTest.next().getBlockId()).isEqualTo(BLOCK_1);
        assertThat(iteratorUnderTest.hasNext()).isFalse();
    }

    @Test
    public void testRetrievesFromRemoteOnly() {
        FallbackToS3ShuffleIterator iteratorUnderTest = getIteratorUnderTest(
                new FetchFailedThrowingStreamList(),
                ImmutableSet.of(BLOCK_INFO_1),
                ImmutableSet.of());
        assertThat(iteratorUnderTest.hasNext()).isTrue();
        assertThat(iteratorUnderTest.next().getBlockId()).isEqualTo(BLOCK_1);
        assertThat(iteratorUnderTest.hasNext()).isFalse();
    }

    @Test
    public void testEmpty() {
        FallbackToS3ShuffleIterator iteratorUnderTest = getIteratorUnderTest(
                new FetchFailedThrowingStreamList(),
                ImmutableSet.of(),
                ImmutableSet.of());
        assertThat(iteratorUnderTest.hasNext()).isFalse();
    }

    @Test
    public void testExecutorThenRemote() {
        TestS3FetcherIterator s3FetcherIterator = new TestS3FetcherIterator(ImmutableSet.of());
        FallbackToS3ShuffleIterator iteratorUnderTest = getIteratorUnderTest(
                new FetchFailedThrowingStreamList().addStream(BLOCK_INFO_1),
                ImmutableSet.of(BLOCK_INFO_2),
                s3FetcherIterator);
        assertThat(iteratorUnderTest.hasNext()).isTrue();
        assertThat(iteratorUnderTest.next().getBlockId()).isEqualTo(BLOCK_1);
        assertThat(iteratorUnderTest.hasNext()).isTrue();
        assertThat(iteratorUnderTest.next().getBlockId()).isEqualTo(BLOCK_2);
        assertThat(iteratorUnderTest.hasNext()).isFalse();
        assertThat(s3FetcherIterator.isInitialized()).isTrue();
        assertThat(s3FetcherIterator.returnedBlockIds()).containsExactly(BLOCK_2);
    }

    @Test
    public void testExecutorFails() {
        TestS3FetcherIterator s3FetcherIterator = new TestS3FetcherIterator(ImmutableSet.of());
        FallbackToS3ShuffleIterator iteratorUnderTest = getIteratorUnderTest(
                new FetchFailedThrowingStreamList()
                        .addThrownFetchFailed(BLOCK_INFO_1)
                        .addStream(BLOCK_INFO_2),
                ImmutableSet.of(),
                s3FetcherIterator);
        assertThat(iteratorUnderTest.hasNext()).isTrue();
        assertThatThrownBy(iteratorUnderTest::next).isInstanceOf(FetchFailedException.class);
        assertThat(s3FetcherIterator.returnedBlockIds()).isEmpty();
        verify(driverEndpointRef, times(1)).blacklistExecutor(
                BLOCK_INFO_1.getShuffleLocation().get());
    }

    private FallbackToS3ShuffleIterator getIteratorUnderTest(
            FetchFailedThrowingStreamList streamsFromExecutors,
            Set<ShuffleBlockInfo> initialRemoteBlocksToFetch,
            S3FetcherIterator s3FetcherIterator) {
        return new FallbackToS3ShuffleIterator(
                streamsFromExecutors.iterator(),
                ImmutableSet.copyOf(streamsFromExecutors.blockInfos()),
                false,
                serializerManager,
                compressionCodec,
                ImmutableSet.copyOf(initialRemoteBlocksToFetch),
                s3FetcherIterator,
                driverEndpointRef);
    }

    private FallbackToS3ShuffleIterator getIteratorUnderTest(
            FetchFailedThrowingStreamList streamsFromExecutors,
            Set<ShuffleBlockInfo> initialRemoteBlocksToFetch,
            Set<ShuffleBlockInfo> remoteStorageFetchFailedBlocks) {
        return getIteratorUnderTest(
                streamsFromExecutors,
                initialRemoteBlocksToFetch,
                new TestS3FetcherIterator(remoteStorageFetchFailedBlocks));
    }

    private static ShuffleBlockInputStream toBlockInputStream(ShuffleBlockInfo block) {
        return new ShuffleBlockInputStream(
                ShuffleBlockId.apply(block.getShuffleId(), block.getMapId(), block.getReduceId()),
                new ByteArrayInputStream(new byte[] { 0, 1, 2, 3, 4, 5 }));
    }

    private static ShuffleBlockInputStream toThrownFetchFailed(ShuffleBlockInfo block) {
        return FetchFailedExceptionThrower.throwFetchFailedException(
                block.getShuffleId(),
                block.getMapId(),
                block.getReduceId(),
                block.getShuffleLocation().orNull(),
                "Manually triggered fetch failed.",
                null);
    }

    private static Supplier<ShuffleBlockInputStream> toBlockInputStreamSupplier(ShuffleBlockInfo block) {
        return () -> toBlockInputStream(block);
    }

    private static Supplier<ShuffleBlockInputStream> toThrowingFetchFailedSupplier(ShuffleBlockInfo block) {
        return () -> toThrownFetchFailed(block);
    }

    private static final class FetchFailedThrowingStreamList implements Iterable<ShuffleBlockInputStream> {

        private final List<ShuffleBlockInfo> blockInfos = new ArrayList<>();
        private final List<Supplier<ShuffleBlockInputStream>> maybeThrowingStreams = new ArrayList<>();

        public FetchFailedThrowingStreamList addStream(ShuffleBlockInfo blockInfo) {
            maybeThrowingStreams.add(toBlockInputStreamSupplier(blockInfo));
            blockInfos.add(blockInfo);
            return this;
        }

        public FetchFailedThrowingStreamList addThrownFetchFailed(ShuffleBlockInfo blockInfo) {
            maybeThrowingStreams.add(toThrowingFetchFailedSupplier(blockInfo));
            blockInfos.add(blockInfo);
            return this;
        }

        @Override
        public Iterator<ShuffleBlockInputStream> iterator() {
            return Iterators.transform(maybeThrowingStreams.iterator(), Supplier::get);
        }

        public List<ShuffleBlockInfo> blockInfos() {
            return blockInfos;
        }
    }

    private static final class TestS3FetcherIterator implements S3FetcherIterator {

        private final Set<ShuffleBlockInfo> remoteFetchFailedBlocks;
        private final Set<BlockId> returnedBlockIds = new HashSet<>();
        private final Set<ShuffleBlockInfo> initializedBlocks = new HashSet<>();
        private Iterator<ShuffleBlockInputStream> backingIterator;

        TestS3FetcherIterator(Set<ShuffleBlockInfo> remoteFetchFailedBlocks) {
            this.remoteFetchFailedBlocks = remoteFetchFailedBlocks;
        }

        @Override
        public void initialize(Collection<ShuffleBlockInfo> blocksToFetch) {
            Preconditions.checkState(!isInitialized(), "Should not be initializing S3 iterator twice.");
            this.initializedBlocks.addAll(blocksToFetch);
            this.backingIterator = ImmutableList.copyOf(blocksToFetch)
                    .stream()
                    .map(block -> {
                        if (remoteFetchFailedBlocks.contains(block)) {
                            return toThrowingFetchFailedSupplier(block).get();
                        } else {
                            return toBlockInputStream(block);
                        }
                    })
                    .iterator();
        }

        @Override
        public boolean isInitialized() {
            return backingIterator != null;
        }

        @Override
        public void cleanup() {}

        @Override
        public boolean hasNext() {
            Preconditions.checkState(
                    isInitialized(),
                    "Iterator must be initialized before calling hasNext()");
            return backingIterator.hasNext();
        }

        @Override
        public ShuffleBlockInputStream next() {
            Preconditions.checkState(
                    isInitialized(),
                    "Iterator must be initialized before calling next()");
            ShuffleBlockInputStream next = backingIterator.next();
            returnedBlockIds.add(next.getBlockId());
            return next;
        }

        public Set<BlockId> returnedBlockIds() {
            return returnedBlockIds;
        }
    }
}
