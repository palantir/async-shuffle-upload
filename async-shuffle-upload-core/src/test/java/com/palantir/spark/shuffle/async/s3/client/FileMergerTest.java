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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.palantir.spark.shuffle.async.s3.client.merging.ShuffleMapInput;
import com.palantir.spark.shuffle.async.s3.merger.FileMerger;
import com.palantir.spark.shuffle.async.s3.metadata.MapOutputId;
import com.palantir.spark.shuffle.async.s3.util.FileSizedInput;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public final class FileMergerTest {

    private static final String FILE_1_CONTENTS = "file1_contents1_a";
    private static final String FILE_2_CONTENTS = "file2_contents2_bbetfs";
    // File3 is empty
    private static final String FILE_4_CONTENTS = "file4_contents4_asdqwttgdsa";
    private static final String[] ALL_FILE_CONTENTS = new String[] {
            FILE_1_CONTENTS, FILE_2_CONTENTS, "", FILE_4_CONTENTS
    };

    private static final int[] MAP_IDS = new int[] { 1, 5, 10, 31 };
    private static final long[] ATTEMPT_IDS = new long[] { 0, 2, 1, 4 };
    private static final int SHUFFLE_ID = 97;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private List<ShuffleMapInput> inputs;

    @Before
    public void before() throws IOException {
        File file1 = tempFolder.newFile();
        File file2 = tempFolder.newFile();
        File file3 = tempFolder.newFile();
        File file4 = tempFolder.newFile();
        writeString(file1, FILE_1_CONTENTS);
        writeString(file2, FILE_2_CONTENTS);
        writeString(file4, FILE_4_CONTENTS);
        inputs = new ArrayList<>();
        File[] files = new File[] { file1, file2, file3, file4 };
        for (int i = 0; i < files.length; i++) {
            int mapId = MAP_IDS[i];
            long attemptId = ATTEMPT_IDS[i];
            File file = files[i];
            ShuffleMapInput shuffleMapInput = new ShuffleMapInput(
                    new MapOutputId(SHUFFLE_ID, mapId, attemptId),
                    new FileSizedInput(file, 128),
                    new FileSizedInput(file, 128));
            inputs.add(shuffleMapInput);
        }
    }

    @Test
    public void testMergeAndSplit() throws IOException {
        ByteArrayOutputStream mergedBytes = new ByteArrayOutputStream();
        try (DataOutputStream mergedOutput = new DataOutputStream(mergedBytes)) {
            FileMerger.mergeMapOutputs(
                    inputs.toArray(new ShuffleMapInput[0]),
                    mergedOutput,
                    ShuffleMapInput::dataSizedInput);
        }
        TestMapOutputFileProvider testMapOutputFileProvider = new TestMapOutputFileProvider(tempFolder);
        ByteArrayInputStream mergedBytesInput = new ByteArrayInputStream(mergedBytes.toByteArray());
        try (DataInputStream mergedBytesDataIn = new DataInputStream(mergedBytesInput)) {
            FileMerger.fetchAndSplitMergedInput(
                    mergedBytesDataIn, testMapOutputFileProvider);
        }
        List<File> createdFiles = testMapOutputFileProvider.getCreatedFiles();
        Assertions.assertThat(createdFiles).hasSize(4);
        List<String> createdFilesContents = createdFiles.stream()
                .map(FileMergerTest::readToString)
                .collect(Collectors.toList());
        Assertions.assertThat(createdFilesContents).containsExactly(ALL_FILE_CONTENTS);
    }

    @Test
    public void testSkipDownloadingExistingFiles() throws IOException {
        ByteArrayOutputStream mergedBytes = new ByteArrayOutputStream();
        try (DataOutputStream mergedOutput = new DataOutputStream(mergedBytes)) {
            FileMerger.mergeMapOutputs(
                    inputs.toArray(new ShuffleMapInput[0]),
                    mergedOutput,
                    ShuffleMapInput::dataSizedInput);
        }
        TestMapOutputFileProvider testMapOutputFileProvider = new TestMapOutputFileProvider(tempFolder, MAP_IDS[0]);

        ByteArrayInputStream mergedBytesInput = new ByteArrayInputStream(mergedBytes.toByteArray());
        try (DataInputStream mergedBytesDataIn = new DataInputStream(mergedBytesInput)) {
            FileMerger.fetchAndSplitMergedInput(
                    mergedBytesDataIn, testMapOutputFileProvider);
        }

        List<File> createdFiles = testMapOutputFileProvider.getCreatedFiles();
        Assertions.assertThat(createdFiles).hasSize(3);
        List<String> createdFilesContents = createdFiles.stream()
                .map(FileMergerTest::readToString)
                .collect(Collectors.toList());
        Assertions.assertThat(createdFilesContents).isEqualTo(
                ImmutableList.copyOf(ALL_FILE_CONTENTS).subList(1, ALL_FILE_CONTENTS.length));
    }

    private static final class TestMapOutputFileProvider implements FileMerger.MapOutputStreamProvider {
        private final TemporaryFolder tempFolder;
        private final List<File> createdFiles;
        private final Set<Integer> mapIdsToSkip;

        TestMapOutputFileProvider(TemporaryFolder tempFolder, int... mapIdsToSkip) {
            this.tempFolder = tempFolder;
            this.createdFiles = new ArrayList<>();
            this.mapIdsToSkip = ImmutableSet.copyOf(Arrays.stream(mapIdsToSkip).iterator());
        }

        @Override
        public Optional<FileMerger.OutputStreamSupplier> getMapOutputStreamIfNotExists(
                int shuffleId, int mapId, long attemptId) {
            if (mapIdsToSkip.contains(mapId)) {
                return Optional.empty();
            }
            File newFile;
            try {
                newFile = tempFolder.newFile(String.format("shuffle-%d-%d-%d", shuffleId, mapId, attemptId));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            createdFiles.add(newFile);
            return Optional.of(() -> new FileOutputStream(newFile));
        }

        public List<File> getCreatedFiles() {
            return createdFiles;
        }
    }

    private static void writeString(File file, String str) {
        try {
            Files.write(str.getBytes(StandardCharsets.UTF_8), file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String readToString(File file) {
        try {
            return new String(Files.toByteArray(file), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
