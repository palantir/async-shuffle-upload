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

import com.palantir.spark.shuffle.async.s3.util.streams.SeekableFileInput;
import com.palantir.spark.shuffle.async.s3.util.streams.SeekableInput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.junit.rules.TemporaryFolder;

public final class TestMergingShuffleFiles implements MergingShuffleFiles {

    private static final int BUFFER_SIZE = 128;

    private final File localFolder;
    private final File remoteFolder;

    private TestMergingShuffleFiles(File localFolder, File remoteFolder) {
        this.localFolder = localFolder;
        this.remoteFolder = remoteFolder;
    }

    public static MergingShuffleFiles fromTempFolder(TemporaryFolder tempFolder) throws IOException {
        File localFolder = tempFolder.newFolder("local-files");
        File remoteFolder = tempFolder.newFolder("remote-files");
        return new TestMergingShuffleFiles(localFolder, remoteFolder);
    }

    @Override
    public SeekableInput getLocalBackupIndexFile(int shuffleId, int mapId, long attemptId) {
        return new SeekableFileInput(resolveLocal(shuffleId, mapId, attemptId, "index"), BUFFER_SIZE);
    }

    @Override
    public SeekableInput getLocalBackupDataFile(int shuffleId, int mapId, long attemptId) {
        return new SeekableFileInput(resolveLocal(shuffleId, mapId, attemptId, "data"), BUFFER_SIZE);
    }

    @Override
    public OutputStream createLocalBackupIndexFile(int shuffleId, int mapId, long attemptId) throws IOException {
        return new FileOutputStream(resolveLocal(shuffleId, mapId, attemptId, "index"));
    }

    @Override
    public OutputStream createLocalBackupDataFile(int shuffleId, int mapId, long attemptId) throws IOException {
        return new FileOutputStream(resolveLocal(shuffleId, mapId, attemptId, "data"));
    }

    @Override
    public boolean doesLocalBackupDataFileExist(int shuffleId, int mapId, long attemptId) {
        return resolveLocal(shuffleId, mapId, attemptId, "data").isFile();
    }

    @Override
    public boolean doesLocalBackupIndexFileExist(int shuffleId, int mapId, long attemptId) {
        return resolveLocal(shuffleId, mapId, attemptId, "index").isFile();
    }

    @Override
    public InputStream openRemoteMergedIndexFile(long mergeId) throws IOException {
        return new FileInputStream(resolveRemote(mergeId, "index"));
    }

    @Override
    public InputStream openRemoteMergedDataFile(long mergeId) throws IOException {
        return new FileInputStream(resolveRemote(mergeId, "data"));
    }

    @Override
    public OutputStream createRemoteMergedIndexFile(long mergeId) throws IOException {
        return new FileOutputStream(resolveRemote(mergeId, "index"));
    }

    @Override
    public OutputStream createRemoteMergedDataFile(long mergeId) throws IOException {
        return new FileOutputStream(resolveRemote(mergeId, "data"));
    }

    private File resolveLocal(int shuffleId, int mapId, long attemptId, String extension) {
        return localFolder.toPath().resolve(
                String.format("%d-%d-%d.%s", shuffleId, mapId, attemptId, extension)).toFile();
    }

    private File resolveRemote(long mergeId, String extension) {
        return remoteFolder.toPath().resolve(String.format("merged-%d.%s", mergeId, extension)).toFile();
    }
}
