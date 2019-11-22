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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.spark.shuffle.async.s3.client.TestClock;
import com.palantir.spark.shuffle.async.s3.metadata.MapOutputId;
import com.palantir.spark.shuffle.async.s3.util.SizedInput;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;
import org.assertj.guava.api.MultimapAssert;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class MergingShuffleUploadCoordinatorTest {

    private static final long BATCH_SIZE_BYTES = 1000L;
    private static final long BATCH_AGE_MILLIS = 500L;
    private static final int MAX_BUFFERED_INPUTS = 100;
    private static final long POLLING_INTERVAL_MILLIS = 1000L;

    @Mock
    private ShuffleDriverEndpointRef driverEndpointRef;

    private MergingShuffleUploadCoordinator coordinatorUnderTest;
    private TestShuffleFileBatchUploader shuffleFileBatchUploader;
    private TestClock clock;
    private DeterministicScheduler uploadCoodinatorExecutor;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        this.clock = new TestClock();
        this.uploadCoodinatorExecutor = new DeterministicScheduler();
        this.shuffleFileBatchUploader = new TestShuffleFileBatchUploader();
        this.coordinatorUnderTest = new MergingShuffleUploadCoordinator(
                BATCH_SIZE_BYTES,
                BATCH_AGE_MILLIS,
                MAX_BUFFERED_INPUTS,
                POLLING_INTERVAL_MILLIS,
                driverEndpointRef,
                MoreExecutors.listeningDecorator(uploadCoodinatorExecutor),
                clock,
                shuffleFileBatchUploader);
        coordinatorUnderTest.start();
        when(driverEndpointRef.isShuffleRegistered(anyInt())).thenReturn(true);
    }

    @Test
    public void whenLargeShuffleFileAdded_uploadsImmediately() {
        tickAndRunCoordinator();
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).isEmpty();
        coordinatorUnderTest.addShuffleMapInputForUpload(newInput(
                0, 0, 0L, BATCH_SIZE_BYTES));
        tickAndRunCoordinator();
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).hasSize(1);
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).containsKeys(0);
        List<ShuffleMapInputBatch> batches = ImmutableList.copyOf(shuffleFileBatchUploader.submittedBatches.get(0));
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0).inputBatch()).hasSize(1);
        assertThat(batches.get(0).mapOutputIds())
                .containsExactly(new MapOutputId(0, 0, 0L));
        assertThat(batches.get(0).totalDataSizeInBytes()).isEqualTo(BATCH_SIZE_BYTES);
    }

    @Test
    public void whenMultipleShuffleFilesAreAdded_doesNotUploadUntilBatchIsBigEnough() {
        coordinatorUnderTest.addShuffleMapInputForUpload(newInput(
                0, 0, 0L, BATCH_SIZE_BYTES / 3L));
        tickAndRunCoordinator();
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).isEmpty();
        coordinatorUnderTest.addShuffleMapInputForUpload(newInput(
                0, 2, 0L, BATCH_SIZE_BYTES / 3L));
        // Test that buffering another input in a different shuffle id doesn't trigger an upload for the previous
        // shuffle id.
        coordinatorUnderTest.addShuffleMapInputForUpload(newInput(
                1, 0, 0L, BATCH_SIZE_BYTES / 2L));
        tickAndRunCoordinator();
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).isEmpty();
        coordinatorUnderTest.addShuffleMapInputForUpload(newInput(
                0, 1, 0L, BATCH_SIZE_BYTES));
        tickAndRunCoordinator();
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).hasSize(1);
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).containsKeys(0);
        List<ShuffleMapInputBatch> batches = ImmutableList.copyOf(shuffleFileBatchUploader.submittedBatches.get(0));
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0).inputBatch()).hasSize(3);
        assertThat(batches.get(0).mapOutputIds())
                .containsExactlyInAnyOrder(
                        new MapOutputId(0, 0, 0L),
                        new MapOutputId(0, 1, 0L),
                        new MapOutputId(0, 2, 0L));
    }

    @Test
    public void whenBufferedInputsAreSaturated_uploadsAllInputs() {
        for (int shuffleId = 0; shuffleId < MAX_BUFFERED_INPUTS; shuffleId++) {
            coordinatorUnderTest.addShuffleMapInputForUpload(newInput(shuffleId, 0, 0L, 1L));
        }
        tickAndRunCoordinator();
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).isEmpty();
        coordinatorUnderTest.addShuffleMapInputForUpload(newInput(
                MAX_BUFFERED_INPUTS, 0, 0L, 1L));
        tickAndRunCoordinator();
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).hasSize(MAX_BUFFERED_INPUTS + 1);
        assertThat(shuffleFileBatchUploader.submittedBatches.asMap())
                .allSatisfy((key, value) -> assertThat(value).hasSize(1));
        assertThat(shuffleFileBatchUploader.submittedBatches.values()
                .stream()
                .flatMap(batch -> Arrays.stream(batch.mapOutputIds()))
                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        IntStream.rangeClosed(0, MAX_BUFFERED_INPUTS)
                                .mapToObj(shuffleId -> new MapOutputId(shuffleId, 0, 0L))
                                .toArray(MapOutputId[]::new));
    }

    @Test
    public void whenInputBatchIsOld_uploadsImmediately() {
        coordinatorUnderTest.addShuffleMapInputForUpload(newInput(0, 0, 0L, 1L));
        tickAndRunCoordinator();
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).isEmpty();
        clock.addMillis(BATCH_AGE_MILLIS + 1);
        tickAndRunCoordinator();
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).hasSize(1);
        assertThat(shuffleFileBatchUploader.submittedBatches.values()).hasSize(1);
        assertThat(Iterables.getOnlyElement(shuffleFileBatchUploader.submittedBatches.values()).mapOutputIds())
                .containsExactly(new MapOutputId(0, 0, 0L));
    }

    @Test
    public void whenShufflesAreNoLongerRegistered_doesNotUpload() {
        coordinatorUnderTest.addShuffleMapInputForUpload(newInput(0, 0, 0L, BATCH_SIZE_BYTES));
        when(driverEndpointRef.isShuffleRegistered(0)).thenReturn(false);
        tickAndRunCoordinator();
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).isEmpty();
        coordinatorUnderTest.addShuffleMapInputForUpload(newInput(1, 0, 0L, 1L));
        tickAndRunCoordinator();
        clock.addMillis(BATCH_AGE_MILLIS + 1);
        when(driverEndpointRef.isShuffleRegistered(1)).thenReturn(false);
        tickAndRunCoordinator();
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).isEmpty();
    }

    @Test
    public void whenExceptionIsThrownInCoordinator_doesNotCancelFuturePollingRounds() {
        coordinatorUnderTest.addShuffleMapInputForUpload(newInput(0, 0, 0L, BATCH_SIZE_BYTES));
        when(driverEndpointRef.isShuffleRegistered(0))
                .thenThrow(IllegalStateException.class)
                .thenReturn(true);
        tickAndRunCoordinator();
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).isEmpty();
        tickAndRunCoordinator();
        multimapAssertThat(shuffleFileBatchUploader.submittedBatches).hasSize(1);
    }

    private void tickAndRunCoordinator() {
        uploadCoodinatorExecutor.tick(POLLING_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        uploadCoodinatorExecutor.runUntilIdle();
    }

    private static <K, V> MultimapAssert<K, V> multimapAssertThat(Multimap<K, V> map) {
        return org.assertj.guava.api.Assertions.assertThat(map);
    }

    private static ShuffleMapInput newInput(
            int shuffleId, int mapId, long attemptId, long dataSize) {
        return new ShuffleMapInput(
                new MapOutputId(shuffleId, mapId, attemptId),
                new MockSizedInput(dataSize),
                new MockSizedInput(dataSize));
    }

    private static final class MockSizedInput implements SizedInput {

        private final long size;

        MockSizedInput(long size) {
            this.size = size;
        }

        @Override
        public long getStreamSizeInBytes() {
            return size;
        }

        @Override
        public InputStream openStream() throws IOException {
            throw new IOException("This test should not be opening streams.");
        }
    }

    private static final class TestShuffleFileBatchUploader implements ShuffleFileBatchUploader {

        private final Multimap<Integer, ShuffleMapInputBatch> submittedBatches = HashMultimap.create();

        @Override
        public void submitBatchUpload(int shuffleId, ShuffleMapInputBatch batch) {
            submittedBatches.put(shuffleId, batch);
        }
    }
}
