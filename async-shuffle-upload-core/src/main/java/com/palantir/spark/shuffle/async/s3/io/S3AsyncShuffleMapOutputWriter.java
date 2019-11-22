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

import com.palantir.spark.shuffle.async.s3.client.ShuffleClient;
import com.palantir.spark.shuffle.async.s3.util.FileSizedInput;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.spark.shuffle.api.MapOutputWriterCommitMessage;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;

public final class S3AsyncShuffleMapOutputWriter implements ShuffleMapOutputWriter {

    private final ShuffleMapOutputWriter delegate;
    private final ShuffleClient shuffleClient;
    private final ShuffleFileLocator shuffleFileLocator;
    private final Set<ShufflePartitionWriter> shufflePartitionWriters;
    private final int shuffleId;
    private final int mapId;
    private final long attemptId;
    private final int fileBufferSize;

    public S3AsyncShuffleMapOutputWriter(
            ShuffleMapOutputWriter delegate,
            ShuffleClient shuffleClient,
            ShuffleFileLocator shuffleFileLocator,
            int shuffleId,
            int mapId,
            long attemptId,
            int fileBufferSize) {
        this.delegate = delegate;
        this.shuffleClient = shuffleClient;
        this.shuffleFileLocator = shuffleFileLocator;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.attemptId = attemptId;
        this.fileBufferSize = fileBufferSize;
        this.shufflePartitionWriters = new HashSet<>();
    }

    @Override
    public ShufflePartitionWriter getPartitionWriter(int partitionId) throws IOException {
        ShufflePartitionWriter writer = delegate.getPartitionWriter(partitionId);
        shufflePartitionWriters.add(writer);
        return writer;
    }

    @Override
    public MapOutputWriterCommitMessage commitAllPartitions() throws IOException {
        MapOutputWriterCommitMessage delegateCommitMessage = delegate.commitAllPartitions();

        File dataFile = shuffleFileLocator.getDataFile(shuffleId, mapId);
        File indexFile = shuffleFileLocator.getIndexFile(shuffleId, mapId);
        if (dataFile.exists()) {
            shuffleClient.asyncWriteDataAndIndexFilesAndClose(
                    new FileSizedInput(dataFile, fileBufferSize),
                    new FileSizedInput(indexFile, fileBufferSize),
                    shuffleId,
                    mapId,
                    attemptId);
        } else {
            shuffleClient.asyncWriteIndexFileAndClose(
                    new FileSizedInput(indexFile, fileBufferSize),
                    shuffleId,
                    mapId,
                    attemptId);
        }

        return delegateCommitMessage;
    }

    @Override
    public void abort(Throwable error) throws IOException {
        delegate.abort(error);
    }
}
