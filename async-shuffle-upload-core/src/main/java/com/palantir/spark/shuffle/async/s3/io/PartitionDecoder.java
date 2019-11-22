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

import com.palantir.spark.shuffle.async.s3.util.streams.SeekableInput;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.spark.network.util.LimitedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PartitionDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionDecoder.class);

    public static InputStream decodePartition(
            SeekableInput data, SeekableInput index, int partitionId) {
        long dataOffset;
        long nextOffset;
        try (InputStream indexInputStream = index.seekToAndOpen(partitionId * 8L);
                DataInputStream indexDataInputStream = new DataInputStream(indexInputStream)) {
            dataOffset = indexDataInputStream.readLong();
            nextOffset = indexDataInputStream.readLong();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        InputStream dataInputStream = null;
        LimitedInputStream limitedDataInputStream = null;
        try {
            dataInputStream = data.seekToAndOpen(dataOffset);
            limitedDataInputStream = new LimitedInputStream(dataInputStream, nextOffset - dataOffset);
        } catch (IOException e) {
            closeQuietly(limitedDataInputStream, e);
            closeQuietly(dataInputStream, e);
            throw new RuntimeException(e);
        }
        return limitedDataInputStream;
    }

    private static void closeQuietly(Closeable closeable, IOException rootException) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                rootException.addSuppressed(e);
                LOGGER.warn("Error encountered when trying to close resource after an error occurred trying to read"
                        + " from it.", e);
            }
        }
    }

    private PartitionDecoder() {}
}
