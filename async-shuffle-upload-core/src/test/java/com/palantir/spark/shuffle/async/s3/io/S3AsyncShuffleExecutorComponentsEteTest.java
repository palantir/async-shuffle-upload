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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.spark.shuffle.async.api.SparkShuffleApiConstants;
import com.palantir.spark.shuffle.async.s3.client.ShuffleStorageStrategy;
import com.palantir.spark.shuffle.async.s3.client.TestClock;
import com.palantir.spark.shuffle.async.s3.metadata.BlacklistedExecutorTracker;
import com.palantir.spark.shuffle.async.s3.metadata.ShuffleIdTracker;
import com.palantir.spark.shuffle.async.s3.metrics.BasicShuffleClientMetrics;
import com.palantir.spark.shuffle.async.s3.metrics.MergingShuffleClientMetrics;
import com.palantir.spark.shuffle.async.s3.metrics.S3AsyncShuffleMetrics;
import com.palantir.spark.shuffle.async.s3.metrics.S3AsyncShuffleMetricsFactory;
import com.palantir.spark.shuffle.async.s3.metrics.S3FetcherIteratorMetrics;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.Optional;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleDataIoSparkConfigs;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleUploadDriverEndpoint;
import org.apache.spark.palantir.shuffle.async.InProcessShuffleDriverEndpointRef;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.api.ShuffleBlockInfo;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.storage.BlockManagerId;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.AdditionalAnswers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(Parameterized.class)
public final class S3AsyncShuffleExecutorComponentsEteTest {

    private static final BlockManagerId MAPPER_LOCATION = BlockManagerId.apply("localhost", 1234);

    private static final byte[][] PARTITIONED_DATA = new byte[][] {
            new byte[] { 0, 4, -1, -10 },
            new byte[] {},
            new byte[] { 15, 2, 7, 23, 5, 11}
    };

    @Parameterized.Parameters
    public static Object[][] parameters() {
        return new Object[][] {
                new Object[] { ShuffleStorageStrategy.BASIC },
                new Object[] { ShuffleStorageStrategy.MERGING }
        };
    }

    private final ShuffleStorageStrategy storageStrategy;

    @Mock
    private SparkEnv sparkEnv;

    @Mock
    private RpcEnv rpcEnv;

    @Mock
    private SerializerManager serializerManager;

    @Mock
    private CompressionCodec compressionCodec;

    @Mock
    private S3AsyncShuffleMetricsFactory metricsFactory;

    @Mock
    private S3AsyncShuffleMetrics metrics;

    @Mock
    private S3FetcherIteratorMetrics s3FetcherIteratorMetrics;

    @Mock
    private BasicShuffleClientMetrics basicShuffleClientMetrics;

    @Mock
    private MergingShuffleClientMetrics mergingShuffleClientMetrics;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private ExecutorService downloadExecutorService;
    private DeterministicScheduler uploadExecutorService;
    private DeterministicScheduler uploadCoordinatorExecutorService;
    private ShuffleFileLocator shuffleFileLocator;
    private SparkConf sparkConf;
    private File localShuffleDir;
    private File remoteShuffleDir;
    private ShuffleExecutorComponents executorComponentsUnderTest;
    private TestClock clock;
    private RpcEndpointRef shuffleDriverEndpointRef;
    private ShuffleIdTracker shuffleIdTracker;
    private BlacklistedExecutorTracker blacklistedExecutorTracker;

    public S3AsyncShuffleExecutorComponentsEteTest(ShuffleStorageStrategy storageStrategy) {
        this.storageStrategy = storageStrategy;
    }

    @Before
    public void before() throws IOException {
        MockitoAnnotations.initMocks(this);
        this.shuffleIdTracker = new ShuffleIdTracker();
        this.uploadExecutorService = new DeterministicScheduler();
        this.downloadExecutorService = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("test-download-%d").build());
        this.uploadCoordinatorExecutorService = new DeterministicScheduler();
        this.localShuffleDir = tempFolder.newFolder("local-shuffles");
        this.remoteShuffleDir = tempFolder.newFolder("remote-shuffles");
        this.sparkConf = new SparkConf()
                .set(AsyncShuffleDataIoSparkConfigs.BASE_URI(),
                        String.format("file://%s", remoteShuffleDir.getAbsolutePath()))
                .set(AsyncShuffleDataIoSparkConfigs.STORAGE_STRATEGY(), storageStrategy.value())
                .set(SparkShuffleApiConstants.SHUFFLE_PLUGIN_APP_NAME_CONF, "spark-app");
        this.shuffleFileLocator = new TestShuffleFileLocator(localShuffleDir);
        ShuffleExecutorComponents delegateTestComponents = new TestShuffleExecutorComponents(shuffleFileLocator);
        this.clock = new TestClock();
        when(metricsFactory.create(eq(sparkConf), any())).thenReturn(metrics);
        when(metrics.s3FetcherIteratorMetrics()).thenReturn(s3FetcherIteratorMetrics);
        when(metrics.basicShuffleClientMetrics()).thenReturn(basicShuffleClientMetrics);
        when(metrics.mergingShuffleClientMetrics()).thenReturn(mergingShuffleClientMetrics);
        this.executorComponentsUnderTest = new S3AsyncShuffleExecutorComponents(
                sparkConf,
                delegateTestComponents,
                java.util.Optional.of(clock),
                () -> sparkEnv,
                () -> shuffleFileLocator,
                () -> compressionCodec,
                java.util.Optional.of(uploadExecutorService),
                java.util.Optional.of(downloadExecutorService),
                java.util.Optional.of(uploadCoordinatorExecutorService),
                java.util.Optional.of(() -> metricsFactory));
        this.blacklistedExecutorTracker = new BlacklistedExecutorTracker();
        this.shuffleDriverEndpointRef = new InProcessShuffleDriverEndpointRef(
                sparkConf,
                new AsyncShuffleUploadDriverEndpoint(rpcEnv, shuffleIdTracker, blacklistedExecutorTracker),
                Executors.newSingleThreadExecutor(
                        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("test-driver-endpoint-%d").build()));
        when(sparkEnv.rpcEnv()).thenReturn(rpcEnv);
        when(rpcEnv.setupEndpointRef(any(), eq(AsyncShuffleUploadDriverEndpoint.NAME())))
                .thenReturn(shuffleDriverEndpointRef);
        when(sparkEnv.serializerManager()).thenReturn(serializerManager);
        when(serializerManager.wrapForEncryption(any(InputStream.class)))
                .thenAnswer(AdditionalAnswers.returnsFirstArg());
        when(serializerManager.wrapForEncryption(any(OutputStream.class)))
                .thenAnswer(AdditionalAnswers.returnsFirstArg());
        when(compressionCodec.compressedInputStream(any())).thenAnswer(AdditionalAnswers.returnsFirstArg());
        when(compressionCodec.compressedOutputStream(any())).thenAnswer(AdditionalAnswers.returnsFirstArg());
        executorComponentsUnderTest.initializeExecutor("test-app", "exec-0", ImmutableMap.of());
    }

    @Test
    public void testWriteAndRead() throws IOException {
        writePartitionedData();
        readAndCheckPartitions(1, 2);
        readAndCheckPartitions(0);
        readAndCheckPartitions(1);
        verify(s3FetcherIteratorMetrics, never()).markFetchFromRemoteSucceeded(
                anyInt(), anyInt(), anyInt(), anyLong());
    }

    @Test
    public void testWrite_whenLocalFileIsDeleted_fallsBackToRemoteFiles() throws IOException {
        writePartitionedData();
        Files.delete(shuffleFileLocator.getDataFile(0, 0).toPath());
        blacklistedExecutorTracker.blacklistExecutor(MAPPER_LOCATION);
        readAndCheckPartitions(1, 2);
        readAndCheckPartitions(0);
        readAndCheckPartitions(1);
        verify(s3FetcherIteratorMetrics, atLeastOnce()).markFetchFromRemoteSucceeded(
                anyInt(), anyInt(), anyInt(), anyLong());
    }

    private void writePartitionedData() throws IOException {
        shuffleIdTracker.registerShuffle(0);
        ShuffleMapOutputWriter mapOutputWriter = executorComponentsUnderTest.createMapOutputWriter(
                0, 0, 0, PARTITIONED_DATA.length);
        for (int partition = 0; partition < PARTITIONED_DATA.length; partition++) {
            byte[] partitionData = PARTITIONED_DATA[partition];
            ShufflePartitionWriter partWriter = mapOutputWriter.getPartitionWriter(partition);
            try (OutputStream partStream = partWriter.openStream()) {
                ByteStreams.copy(new ByteArrayInputStream(partitionData), partStream);
            }
            assertThat(partWriter.getNumBytesWritten()).isEqualTo(partitionData.length);
        }
        mapOutputWriter.commitAllPartitions();
        clock.addMillis(Long.MAX_VALUE / 2);
        uploadCoordinatorExecutorService.tick(1, TimeUnit.MINUTES);
        uploadCoordinatorExecutorService.runUntilIdle();
        uploadExecutorService.runUntilIdle();
    }

    private void readAndCheckPartitions(Integer... partitions) throws IOException {
        List<ShuffleBlockInfo> blocksToFetch = Arrays.stream(partitions)
                .map(partId -> new ShuffleBlockInfo(
                        0,
                        0,
                        partId,
                        (long) PARTITIONED_DATA[partId].length,
                        0L,
                        Optional.of(MAPPER_LOCATION)))
                .collect(Collectors.toList());
        Set<List<Byte>> partBytes =
                ImmutableSet.copyOf(
                        StreamSupport.stream(executorComponentsUnderTest
                                .getPartitionReaders(blocksToFetch)
                                .spliterator(), false)
                                .map(partStream -> {
                                    byte[] partByteArray;
                                    try (InputStream closingPartStream = partStream) {
                                        partByteArray = ByteStreams.toByteArray(closingPartStream);
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return toList(partByteArray);
                                }).collect(Collectors.toList()));
        Set<List<Byte>> expectedPartitions = Arrays.stream(partitions)
                .map(part -> toList(PARTITIONED_DATA[part]))
                .collect(Collectors.toSet());
        assertThat(partBytes).isEqualTo(expectedPartitions);
    }

    private static List<Byte> toList(byte[] byteArray) {
        List<Byte> byteList = new ArrayList<>(byteArray.length);
        for (byte byteElement : byteArray) {
            byteList.add(byteElement);
        }
        return byteList;
    }

    private static final class TestShuffleFileLocator implements ShuffleFileLocator {
        private final File shuffleDir;

        private TestShuffleFileLocator(File shuffleDir) {
            this.shuffleDir = shuffleDir;
        }

        @Override
        public File getDataFile(int shuffleId, int mapId) {
            return resolveShuffleDir(shuffleId, mapId).resolve("shuffle.data").toFile();
        }

        @Override
        public File getIndexFile(int shuffleId, int mapId) {
            return resolveShuffleDir(shuffleId, mapId).resolve("shuffle.index").toFile();
        }

        private Path resolveShuffleDir(int shuffleId, int mapId) {
            return shuffleDir
                    .toPath()
                    .resolve(Integer.toString(shuffleId))
                    .resolve(Integer.toString(mapId));
        }
    }
}
