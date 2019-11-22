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

package com.palantir.spark.shuffle.async.s3.merger;

import com.google.common.io.ByteStreams;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.spark.shuffle.async.s3.client.merging.ShuffleMapInput;
import com.palantir.spark.shuffle.async.s3.util.SizedInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.function.Function;
import org.apache.spark.network.util.LimitedInputStream;

/**
 * Utility for merging map output files together and for splicing a merged file into its original parts.
 * <p>
 * The merged file has the following format:
 * <p>
 * <code>numMapOutputs|fileHeader1|file1Contents|fileHeader2|file2Contents|...fileHeaderN|fileNContents|EOF</code>
 * <p>
 * where each fileHeader has the format:
 * <p>
 * <code>fileMapId|fileMapAttemptId|fileSize</code>
 * <p>
 * and fileMapId is an integer, while fileSize is a long.
 * <p>
 * This would be used to merge both the data and the index files from map outputs.
 */
public final class FileMerger {

    /**
     * Combine an array of files and pushes the merged data to the provided output stream.
     */
    public static void mergeMapOutputs(
            ShuffleMapInput[] inputs,
            DataOutputStream mergedOutput,
            Function<ShuffleMapInput, SizedInput> inputFileExtractor) throws IOException {
        mergedOutput.writeInt(inputs.length);

        for (ShuffleMapInput input : inputs) {
            int shuffleId = input.mapOutputId().shuffleId();
            int mapId = input.mapOutputId().mapId();
            long attemptId = input.mapOutputId().mapAttemptId();
            SizedInput stream = inputFileExtractor.apply(input);
            mergedOutput.writeInt(shuffleId);
            mergedOutput.writeInt(mapId);
            mergedOutput.writeLong(attemptId);
            mergedOutput.writeLong(stream.getStreamSizeInBytes());
            if (stream.getStreamSizeInBytes() > 0L) {
                try (InputStream inputStream = stream.openStream()) {
                    ByteStreams.copy(inputStream, mergedOutput);
                }
            }
        }
    }

    /**
     * Takes an input stream that provides bytes written by
     * {@link #mergeMapOutputs(ShuffleMapInput[], DataOutputStream, Function)}, and extracts the bytes to
     * their
     * original split form.
     * <p>
     * This downloads all of the contents from the given input stream. This assumes that it is more optimal to fetch
     * all the data to local disk before working with it. An alternative approach would be to only fetch the chunks
     * of the merged data that are required, but this would result in multiple requests to the remote storage layer.
     * It's worth investigating the alternative data access strategy in the future.
     */
    public static void fetchAndSplitMergedInput(
            DataInputStream mergedInput,
            MapOutputStreamProvider mapOutputStreamProvider) throws IOException {
        int numMapIds = mergedInput.readInt();

        for (int i = 0; i < numMapIds; i++) {
            int shuffleId = mergedInput.readInt();
            int mapId = mergedInput.readInt();
            long attemptId = mergedInput.readLong();
            long mapFileSize = mergedInput.readLong();
            Optional<OutputStreamSupplier> maybeOutputStream = mapOutputStreamProvider.getMapOutputStreamIfNotExists(
                    shuffleId, mapId, attemptId);
            if (maybeOutputStream.isPresent()) {
                try (OutputStream mapOut = maybeOutputStream.get().openStream();
                        LimitedInputStream limitedInput = new LimitedInputStream(
                                mergedInput, mapFileSize, false)) {
                    ByteStreams.copy(limitedInput, mapOut);
                }
            } else {
                long skipped = mergedInput.skip(mapFileSize);
                Preconditions.checkState(
                        skipped == mapFileSize,
                        "Unexpected number of bytes skipped during download when the local file already"
                                + " existed.",
                        SafeArg.of("expectedBytesToSkip", mapFileSize),
                        SafeArg.of("actualBytesSkipped", skipped));
            }
        }
    }

    @FunctionalInterface
    public interface MapOutputStreamProvider {
        Optional<OutputStreamSupplier> getMapOutputStreamIfNotExists(
                int shuffleId, int mapId, long attemptId) throws IOException;
    }

    @FunctionalInterface
    public interface OutputStreamSupplier {
        OutputStream openStream() throws IOException;
    }

    private FileMerger() {}
}
