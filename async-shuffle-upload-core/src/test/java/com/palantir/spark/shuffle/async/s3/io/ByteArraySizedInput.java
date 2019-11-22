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

import com.palantir.spark.shuffle.async.s3.util.SizedInput;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public final class ByteArraySizedInput implements SizedInput {

    private final byte[] bytes;

    public ByteArraySizedInput(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public long getStreamSizeInBytes() {
        return bytes.length;
    }

    @Override
    public InputStream openStream() throws IOException {
        return new ByteArrayInputStream(bytes);
    }
}
