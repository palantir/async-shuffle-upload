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

package com.palantir.spark.shuffle.async.s3.util.streams;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;

public final class SeekableFileInput implements SeekableInput {

    private final File file;
    private final int bufferSize;

    public SeekableFileInput(File file, int bufferSize) {
        this.file = file;
        this.bufferSize = bufferSize;
    }

    @Override
    public InputStream seekToAndOpen(long startPosition) throws IOException {
        SeekableByteChannel channel = Files.newByteChannel(file.toPath());
        channel.position(startPosition);
        return new BufferedInputStream(
                Channels.newInputStream(channel),
                bufferSize);
    }
}
