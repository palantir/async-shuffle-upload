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

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.spark.shuffle.async.s3.util.streams.SeekableFileInput;
import com.palantir.spark.shuffle.async.s3.util.streams.SeekableInput;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simpler wrapper around local and remote file systems for the merging shuffle file storage strategy.
 */
public final class DefaultMergingShuffleFiles implements MergingShuffleFiles {

    private static final String MERGED_DATA_PREFIX = "merged-data";

    private final String appId;
    private final int localFileBufferSize;
    private final int networkFileBufferSize;
    private final URI remoteBackupBaseUri;
    private final FileSystem fs;
    private final File downloadedBackupsDir;

    private DefaultMergingShuffleFiles(
            String appId,
            int localFileBufferSize,
            int networkFileBufferSize,
            URI remoteBackupBaseUri,
            FileSystem fs,
            File downloadedBackupsDir) {
        this.appId = appId;
        this.localFileBufferSize = localFileBufferSize;
        this.networkFileBufferSize = networkFileBufferSize;
        this.remoteBackupBaseUri = remoteBackupBaseUri;
        this.fs = fs;
        this.downloadedBackupsDir = downloadedBackupsDir;
    }

    public static MergingShuffleFiles mergingShuffleFiles(
            String appId,
            int localFileBufferSize,
            int networkFileBufferSize,
            URI remoteBackupBaseUri,
            FileSystem fs) {
        File downloadedBackupsDir;
        try {
            downloadedBackupsDir = Files.createTempDirectory("downloaded-shuffle-backups").toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new DefaultMergingShuffleFiles(
                appId, localFileBufferSize, networkFileBufferSize, remoteBackupBaseUri, fs, downloadedBackupsDir);
    }

    @Override
    public OutputStream createRemoteMergedDataFile(long mergeId) throws IOException {
        return new BufferedOutputStream(fs.create(mergedDataPath(mergeId)), networkFileBufferSize);
    }

    @Override
    public OutputStream createRemoteMergedIndexFile(long mergeId) throws IOException {
        return new BufferedOutputStream(fs.create(mergedIndexPath(mergeId)), networkFileBufferSize);
    }

    @Override
    public InputStream openRemoteMergedDataFile(long mergeId) throws IOException {
        return new BufferedInputStream(fs.open(mergedDataPath(mergeId)), networkFileBufferSize);
    }

    @Override
    public InputStream openRemoteMergedIndexFile(long mergeId) throws IOException {
        return new BufferedInputStream(fs.open(mergedIndexPath(mergeId)), networkFileBufferSize);
    }

    @Override
    public boolean doesLocalBackupIndexFileExist(int shuffleId, int mapId, long attemptId) {
        return localBackupIndexFile(shuffleId, mapId, attemptId).isFile();
    }

    @Override
    public boolean doesLocalBackupDataFileExist(int shuffleId, int mapId, long attemptId) {
        return localBackupDataFile(shuffleId, mapId, attemptId).isFile();
    }

    @Override
    public OutputStream createLocalBackupDataFile(int shuffleId, int mapId, long attemptId) throws IOException {
        return createTempBackupFile(localBackupDataFile(shuffleId, mapId, attemptId));
    }

    @Override
    public OutputStream createLocalBackupIndexFile(int shuffleId, int mapId, long attemptId) throws IOException {
        return createTempBackupFile(localBackupIndexFile(shuffleId, mapId, attemptId));
    }

    @Override
    public SeekableInput getLocalBackupDataFile(int shuffleId, int mapId, long attemptId) {
        return new SeekableFileInput(localBackupDataFile(shuffleId, mapId, attemptId), localFileBufferSize);
    }

    @Override
    public SeekableInput getLocalBackupIndexFile(int shuffleId, int mapId, long attemptId) {
        return new SeekableFileInput(localBackupIndexFile(shuffleId, mapId, attemptId), localFileBufferSize);
    }

    private OutputStream createTempBackupFile(File finalFile) throws IOException {
        File temp = Utils.tempFileWith(finalFile);
        return new BufferedOutputStream(new TempFileOutputStream(temp, finalFile), localFileBufferSize);
    }

    private Path mergedDataPath(long mergeId) {
        return mergedFilePath("data", mergeId);
    }

    private Path mergedIndexPath(long mergeId) {
        return mergedFilePath("index", mergeId);
    }

    private Path mergedFilePath(String extension, long mergeId) {
        return new Path(new Path(remoteBackupBaseUri),
                String.format(
                        "%s/appId=%s/merged-%d.%s",
                        MERGED_DATA_PREFIX,
                        appId,
                        mergeId,
                        extension));
    }

    private File localBackupDataFile(int shuffleId, int mapId, long attemptId) {
        return toLocalBackupFile(shuffleId, mapId, attemptId, "data");
    }

    private File localBackupIndexFile(int shuffleId, int mapId, long attemptId) {
        return toLocalBackupFile(shuffleId, mapId, attemptId, "index");
    }

    private File toLocalBackupFile(int shuffleId, int mapId, long attemptId, String extension) {
        java.nio.file.Path dataDir =
                downloadedBackupsDir.toPath()
                        .resolve(String.format("shuffle=%d", shuffleId))
                        .resolve(String.format("map=%d", mapId))
                        .resolve(String.format("attempt=%d", attemptId));
        if (!dataDir.toFile().isDirectory()) {
            try {
                Files.createDirectories(dataDir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return dataDir.resolve(String.format("shuffle.%s", extension)).toFile();
    }

    private static final class TempFileOutputStream extends FilterOutputStream {

        private static final Logger LOGGER = LoggerFactory.getLogger(TempFileOutputStream.class);
        private final File tempFile;
        private final File outputFile;

        private boolean isClosed;

        TempFileOutputStream(File tempFile, File outputFile) throws IOException {
            super(Files.newOutputStream(tempFile.toPath(), StandardOpenOption.CREATE_NEW));
            this.tempFile = tempFile;
            this.outputFile = outputFile;
            this.isClosed = false;
        }

        @Override
        public void close() throws IOException {
            if (!isClosed) {
                super.close();
                Preconditions.checkState(tempFile.isFile(), "Temporary file was not created.");
                try {
                    Files.move(tempFile.toPath(), outputFile.toPath());
                } catch (FileAlreadyExistsException e) {
                    LOGGER.warn("Map output file was already created; perhaps another thread"
                            + " accidentally downloaded the same data.",
                            SafeArg.of("mapOutputFilePath", outputFile.getAbsolutePath()),
                            e);
                }
                isClosed = true;
            }
        }
    }
}
