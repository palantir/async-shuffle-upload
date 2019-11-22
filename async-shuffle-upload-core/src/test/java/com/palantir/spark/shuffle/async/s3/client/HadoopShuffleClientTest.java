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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.spark.shuffle.async.s3.client.basic.HadoopShuffleClient;
import com.palantir.spark.shuffle.async.s3.metrics.slf4j.Slf4jBasicShuffleClientMetrics;
import com.palantir.spark.shuffle.async.s3.util.FileSizedInput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Clock;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.http.client.utils.URIBuilder;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class HadoopShuffleClientTest {

    private static final int NUM_PARTITIONS = 7;
    private static final int MAX_PARTITION_SIZE_BYTES = 1000;
    private static final String APP_ID = "my-app";
    private static final int SHUFFLE_ID = 10;
    private static final int MAP_ID = 1;
    private static final int ATTEMPT_ID = 0;

    private static final DeterministicScheduler downloadExecService = new DeterministicScheduler();
    private static final DeterministicScheduler uploadExecService = new DeterministicScheduler();

    private static final Random random = new Random();

    @Mock
    private ShuffleDriverEndpointRef mockDriverEndpointRef;

    private HadoopShuffleClient s3Client;

    private byte[][] data;
    private File dataFile;
    private File indexFile;
    private File uploadFolder;

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Before
    public void before() throws Exception {
        MockitoAnnotations.initMocks(this);
        data = new byte[NUM_PARTITIONS][];
        dataFile = tempDir.newFile();
        indexFile = tempDir.newFile();
        uploadFolder = tempDir.newFolder();

        long lengthSoFar = 0;

        try (OutputStream dataInput = new FileOutputStream(dataFile);
                DataOutputStream indexInput = new DataOutputStream(new FileOutputStream(indexFile))) {
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                int size = random.nextInt(MAX_PARTITION_SIZE_BYTES);
                byte[] randomBytes = new byte[size];
                random.nextBytes(randomBytes);
                dataInput.write(randomBytes);
                indexInput.writeLong(lengthSoFar);
                lengthSoFar += size;
                data[i] = randomBytes;
            }
        }
        URI baseUri = new URIBuilder().setScheme("file").setPath(uploadFolder.getAbsolutePath()).build();
        s3Client = new HadoopShuffleClient(
                baseUri,
                APP_ID,
                MoreExecutors.listeningDecorator(uploadExecService),
                MoreExecutors.listeningDecorator(downloadExecService),
                FileSystem.get(baseUri, new Configuration()),
                new Slf4jBasicShuffleClientMetrics(APP_ID),
                mockDriverEndpointRef,
                1000,
                Clock.systemUTC());
    }

    @Test
    public void testWriteAndRead() throws Exception {
        when(mockDriverEndpointRef.isShuffleRegistered(SHUFFLE_ID)).thenReturn(true);
        s3Client.asyncWriteDataAndIndexFilesAndClose(
                new FileSizedInput(dataFile, 128),
                new FileSizedInput(indexFile, 128),
                SHUFFLE_ID,
                MAP_ID,
                ATTEMPT_ID);
        uploadExecService.runUntilIdle();
        checkPartitionData(0);
        checkPartitionData(4);
    }

    @Test
    public void testShuffleDoesNotExist() {
        when(mockDriverEndpointRef.isShuffleRegistered(SHUFFLE_ID)).thenReturn(false);
        s3Client.asyncWriteDataAndIndexFilesAndClose(
                new FileSizedInput(dataFile, 128),
                new FileSizedInput(indexFile, 128),
                SHUFFLE_ID,
                MAP_ID,
                ATTEMPT_ID);
        uploadExecService.runUntilIdle();

        assertThatThrownBy(() -> checkPartitionData(0))
                .hasRootCauseExactlyInstanceOf(FileNotFoundException.class);
    }

    private void checkPartitionData(int partitionId) throws ExecutionException, InterruptedException, IOException {
        ListenableFuture<InputStream> inputStreamFuture =
                s3Client.getBlockData(SHUFFLE_ID, MAP_ID, partitionId, ATTEMPT_ID);
        downloadExecService.runUntilIdle();
        InputStream inputStream = inputStreamFuture.get();

        byte[] partitionData = data[partitionId];
        byte[] readBytes = ByteStreams.toByteArray(inputStream);
        assertThat(readBytes).containsExactly(partitionData);
    }
}
