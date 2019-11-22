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
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.SettableFuture;
import com.palantir.spark.shuffle.async.s3.client.ShuffleClient;
import com.palantir.spark.shuffle.async.s3.metrics.S3FetcherIteratorMetrics;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.spark.api.java.Optional;
import org.apache.spark.shuffle.FetchFailedException;
import org.apache.spark.shuffle.api.ShuffleBlockInfo;
import org.apache.spark.shuffle.api.ShuffleBlockInputStream;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.ShuffleBlockId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class DefaultS3FetcherIteratorTest {

    @Mock
    private ShuffleClient shuffleClient;
    @Mock
    private S3FetcherIteratorMetrics metrics;
    @Mock
    private ShuffleBlockInputStream inputStream;

    private static final ExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private static final ScheduledExecutorService timeoutExecutorService = Executors.newScheduledThreadPool(1);
    private static final int SHUFFLE_ID = 0;
    private static final int MAP_ID = 10;
    private static final int REDUCE_ID = 4;
    private static final long ATTEMPT_ID = 3;
    private static final ShuffleBlockId SHUFFLE_BLOCK_ID =
            ShuffleBlockId.apply(SHUFFLE_ID, MAP_ID, REDUCE_ID);
    private static final BlockManagerId BLOCK_MANAGER_ID = BlockManagerId.apply("host", 1234);
    private static final ShuffleBlockInfo SHUFFLE_BLOCK_INFO =
            new ShuffleBlockInfo(SHUFFLE_ID, MAP_ID, REDUCE_ID, 10, ATTEMPT_ID, Optional.of(BLOCK_MANAGER_ID));

    private DefaultS3FetcherIterator fetcherIterator;
    private SettableFuture<InputStream> inputStreamFuture;
    private Set<ShuffleBlockInfo> shuffleBlocksToFetch;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        fetcherIterator = new DefaultS3FetcherIterator(shuffleClient, executorService, timeoutExecutorService, metrics);
        inputStreamFuture = SettableFuture.create();
        when(shuffleClient.getBlockData(SHUFFLE_ID, MAP_ID, REDUCE_ID, ATTEMPT_ID)).thenReturn(inputStreamFuture);
        shuffleBlocksToFetch = new HashSet<>();
        shuffleBlocksToFetch.add(SHUFFLE_BLOCK_INFO);
    }


    @Test
    public void testCantInitializeTwice() {
        fetcherIterator.initialize(Collections.emptySet());
        assertThatThrownBy(() -> fetcherIterator.initialize(Collections.emptySet()))
                .hasMessageContaining("DefaultS3FetcherIterator can only be initialized once");
    }

    @Test
    public void testGetsBlock() {
        fetcherIterator.initialize(shuffleBlocksToFetch);
        inputStreamFuture.set(inputStream);
        ShuffleBlockInputStream actualInputStream = fetcherIterator.next();
        verify(shuffleClient).getBlockData(SHUFFLE_ID, MAP_ID, REDUCE_ID, ATTEMPT_ID);
        assertThat(actualInputStream.getBlockId()).isEqualTo(SHUFFLE_BLOCK_ID);

        // there's nothing else left
        assertThatThrownBy(() -> fetcherIterator.next())
                .hasMessageContaining("Next should not be called because iterator is empty");
    }

    @Test
    public void testFetchFails() {
        fetcherIterator.initialize(shuffleBlocksToFetch);
        inputStreamFuture.setException(new Exception("Could not fetch from S3"));
        assertThatThrownBy(() -> fetcherIterator.next())
                .isInstanceOf(FetchFailedException.class)
                .hasMessageContaining("Exception thrown when fetching data from remote storage.");
    }

    @Test
    public void testFuturesCancelledWhenStopped() {
        fetcherIterator = new DefaultS3FetcherIterator(
                shuffleClient, timeoutExecutorService, timeoutExecutorService, metrics);
        fetcherIterator.initialize(shuffleBlocksToFetch);
        // stop the fetcher iterator
        fetcherIterator.cleanup();
        assertThat(inputStreamFuture.isCancelled()).isTrue();
    }

    @Test
    public void testFetchedStreamsDontLeak() throws IOException {
        fetcherIterator.initialize(shuffleBlocksToFetch);
        inputStreamFuture.set(inputStream);
        fetcherIterator.cleanup();
        verify(inputStream, times(1)).close();
    }

    @Test
    public void testFetchedFailedStreamsInCleanup() throws IOException {
        fetcherIterator.initialize(shuffleBlocksToFetch);
        inputStreamFuture.setException(new Exception("Could not fetch from S3"));
        fetcherIterator.cleanup();
    }

    @Test
    public void testClosingMultipleThings() throws IOException {
        ShuffleBlockInfo shuffleBlock1 =
                new ShuffleBlockInfo(SHUFFLE_ID, MAP_ID, 1, 0, 0, Optional.of(BLOCK_MANAGER_ID));
        ShuffleBlockInfo shuffleBlock2 =
                new ShuffleBlockInfo(SHUFFLE_ID, MAP_ID, 2, 0, 0, Optional.of(BLOCK_MANAGER_ID));
        SettableFuture<InputStream> inputStreamFuture1 = SettableFuture.create();
        SettableFuture<InputStream> inputStreamFuture2 = SettableFuture.create();
        shuffleBlocksToFetch.add(shuffleBlock1);
        shuffleBlocksToFetch.add(shuffleBlock2);
        when(shuffleClient.getBlockData(SHUFFLE_ID, MAP_ID, 1, 0)).thenReturn(inputStreamFuture1);
        when(shuffleClient.getBlockData(SHUFFLE_ID, MAP_ID, 2, 0)).thenReturn(inputStreamFuture2);

        fetcherIterator.initialize(shuffleBlocksToFetch);
        inputStreamFuture1.set(inputStream);
        inputStreamFuture.setException(new Exception("Could not fetch from S3"));

        fetcherIterator.cleanup();
        verify(inputStream, times(1)).close();
        assertThat(inputStreamFuture2).isCancelled();
    }
}
