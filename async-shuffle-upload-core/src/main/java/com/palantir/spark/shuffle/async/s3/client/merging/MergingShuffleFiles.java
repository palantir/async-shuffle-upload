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

import com.palantir.spark.shuffle.async.s3.util.streams.SeekableInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface MergingShuffleFiles {

    SeekableInput getLocalBackupIndexFile(int shuffleId, int mapId, long attemptId);

    SeekableInput getLocalBackupDataFile(int shuffleId, int mapId, long attemptId);

    OutputStream createLocalBackupIndexFile(int shuffleId, int mapId, long attemptId) throws IOException;

    OutputStream createLocalBackupDataFile(int shuffleId, int mapId, long attemptId) throws IOException;

    boolean doesLocalBackupDataFileExist(int shuffleId, int mapId, long attemptId);

    boolean doesLocalBackupIndexFileExist(int shuffleId, int mapId, long attemptId);

    default boolean doLocalBackupsExist(int shuffleId, int mapId, long attemptId) {
        return doesLocalBackupDataFileExist(shuffleId, mapId, attemptId)
                && doesLocalBackupIndexFileExist(shuffleId, mapId, attemptId);
    }

    InputStream openRemoteMergedIndexFile(long mergeId) throws IOException;

    InputStream openRemoteMergedDataFile(long mergeId) throws IOException;

    OutputStream createRemoteMergedIndexFile(long mergeId) throws IOException;

    OutputStream createRemoteMergedDataFile(long mergeId) throws IOException;
}
