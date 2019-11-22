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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.spark.shuffle.async.s3.io.ByteArraySizedInput;
import com.palantir.spark.shuffle.async.s3.merger.FileMerger;
import com.palantir.spark.shuffle.async.s3.metadata.MapOutputId;
import com.palantir.spark.shuffle.async.s3.metrics.slf4j.Slf4jMergingShuffleClientMetrics;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class ShuffleFileBatchUploaderTest {

    private static final String APP_ID = "test-app";
    private static final byte[] FIRST_PART_DATA_BYTES = new byte[] { 0, 6, 14, 2 };
    private static final byte[] FIRST_PART_INDEX_BYTES = new byte[] { 0, 2 };
    private static final byte[] SECOND_PART_DATA_BYTES = new byte[] {};
    private static final byte[] SECOND_PART_INDEX_BYTES = new byte[] {};
    private static final byte[] THIRD_PART_DATA_BYTES = new byte[] { 9, 10, 23, 5, 102 };
    private static final byte[] THIRD_PART_INDEX_BYTES = new byte[] { 0, 3 };

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Mock
    private ShuffleDriverEndpointRef driverEndpointRef;

    private ShuffleFileBatchUploader uploaderUnderTest;
    private MergingShuffleFiles shuffleFiles;
    private DeterministicScheduler uploadExecService;
    private ShuffleMapInputBatch batchToUpload;

    @Before
    public void before() throws IOException {
        MockitoAnnotations.initMocks(this);
        this.shuffleFiles = TestMergingShuffleFiles.fromTempFolder(tempFolder);
        this.uploadExecService = new DeterministicScheduler();
        this.uploaderUnderTest = new DefaultShuffleFileBatchUploader(
                APP_ID,
                driverEndpointRef,
                shuffleFiles,
                MoreExecutors.listeningDecorator(uploadExecService),
                new Slf4jMergingShuffleClientMetrics("app-name"),
                Clock.systemUTC());
        this.batchToUpload = new ShuffleMapInputBatch()
                .addInput(
                        new ShuffleMapInput(
                                new MapOutputId(0, 0, 0L),
                        new ByteArraySizedInput(FIRST_PART_DATA_BYTES),
                        new ByteArraySizedInput(FIRST_PART_INDEX_BYTES)),
                        System.currentTimeMillis())
                .addInput(
                        new ShuffleMapInput(
                                new MapOutputId(0, 1, 0L),
                                new ByteArraySizedInput(SECOND_PART_DATA_BYTES),
                                new ByteArraySizedInput(SECOND_PART_INDEX_BYTES)),
                        System.currentTimeMillis())
                .addInput(
                        new ShuffleMapInput(
                                new MapOutputId(0, 2, 0L),
                                new ByteArraySizedInput(THIRD_PART_DATA_BYTES),
                                new ByteArraySizedInput(THIRD_PART_INDEX_BYTES)),
                        System.currentTimeMillis());
    }

    @Test
    public void testMergedUpload_cancelsIfShuffleNotRegistered() {
        when(driverEndpointRef.isShuffleRegistered(0)).thenReturn(false);
        uploaderUnderTest.submitBatchUpload(0, batchToUpload);
        uploadExecService.runUntilIdle();
        assertThat(driverEndpointRef.getMapOutputMergeId(new MapOutputId(0, 0, 0L)))
                .isEmpty();
    }

    @Test
    public void testMergedUpload() throws IOException {
        when(driverEndpointRef.isShuffleRegistered(0)).thenReturn(true);
        when(driverEndpointRef.getNextMergeId()).thenReturn(0L);
        uploaderUnderTest.submitBatchUpload(0, batchToUpload);
        uploadExecService.runUntilIdle();
        verify(driverEndpointRef).registerMergedMapOutput(Arrays.asList(batchToUpload.mapOutputIds()), 0L);
        TestMapOutputStreamProvider dataStreamProvider = new TestMapOutputStreamProvider(tempFolder);
        try (InputStream mergedInput = shuffleFiles.openRemoteMergedDataFile(0);
                DataInputStream dataMergedInput = new DataInputStream(mergedInput)) {
            FileMerger.fetchAndSplitMergedInput(dataMergedInput, dataStreamProvider);
        }
        assertThat(dataStreamProvider.downloadedFiles).hasSize(3);

        TestMapOutputStreamProvider indexStreamProvider = new TestMapOutputStreamProvider(tempFolder);
        try (InputStream mergedInput = shuffleFiles.openRemoteMergedIndexFile(0);
                DataInputStream dataMergedInput = new DataInputStream(mergedInput)) {
            FileMerger.fetchAndSplitMergedInput(dataMergedInput, indexStreamProvider);
        }
        assertThat(indexStreamProvider.downloadedFiles).hasSize(3);

        checkDownloadedDataFiles(dataStreamProvider);
        checkDownloadedIndexFiles(indexStreamProvider);
    }

    private void checkDownloadedDataFiles(TestMapOutputStreamProvider dataStreamProvider) {
        dataStreamProvider.downloadedFiles.forEach((mapOutputId, file) -> {
            assertThat(mapOutputId).isIn(Arrays.asList(batchToUpload.mapOutputIds()));
            ShuffleMapInput originalInput = Arrays.stream(batchToUpload.inputBatch())
                    .filter(input -> input.mapOutputId().equals(mapOutputId))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Original map input is missing."));
            byte[] originalInputBytes;
            try (InputStream originalInputStream = originalInput.dataSizedInput().openStream()) {
                originalInputBytes = ByteStreams.toByteArray(originalInputStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            byte[] downloadedBytes;
            try {
                downloadedBytes = Files.toByteArray(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            assertThat(downloadedBytes).containsExactly(originalInputBytes);
        });
    }

    private void checkDownloadedIndexFiles(TestMapOutputStreamProvider indexStreamProvider) {
        indexStreamProvider.downloadedFiles.forEach((mapOutputId, file) -> {
            assertThat(mapOutputId).isIn(Arrays.asList(batchToUpload.mapOutputIds()));
            ShuffleMapInput originalInput = Arrays.stream(batchToUpload.inputBatch())
                    .filter(input -> input.mapOutputId().equals(mapOutputId))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Original map input is missing."));
            byte[] originalInputBytes;
            try (InputStream originalInputStream = originalInput.indexSizedInput().openStream()) {
                originalInputBytes = ByteStreams.toByteArray(originalInputStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            byte[] downloadedBytes;
            try {
                downloadedBytes = Files.toByteArray(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            assertThat(downloadedBytes).containsExactly(originalInputBytes);
        });
    }

    private static final class TestMapOutputStreamProvider implements FileMerger.MapOutputStreamProvider {
        private final TemporaryFolder downloadTempFolder;
        private final Map<MapOutputId, File> downloadedFiles;

        private TestMapOutputStreamProvider(TemporaryFolder downloadTempFolder) {
            this.downloadTempFolder = downloadTempFolder;
            this.downloadedFiles = new HashMap<>();
        }

        @Override
        public Optional<FileMerger.OutputStreamSupplier> getMapOutputStreamIfNotExists(
                int shuffleId, int mapId, long attemptId) throws IOException {
            File outputFile = downloadTempFolder.newFile();
            downloadedFiles.put(new MapOutputId(shuffleId, mapId, attemptId), outputFile);
            return Optional.of(() -> new FileOutputStream(outputFile));
        }
    }
}
