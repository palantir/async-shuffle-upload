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

import com.palantir.spark.shuffle.async.s3.metadata.MapOutputId;
import com.palantir.spark.shuffle.async.s3.util.SizedInput;

public final class ShuffleMapInput {
    private final MapOutputId mapOutputId;
    private final SizedInput dataSizedInput;
    private final SizedInput indexSizedInput;

    public ShuffleMapInput(
            MapOutputId mapOutputId,
            SizedInput dataSizedInput,
            SizedInput indexSizedInput) {
        this.mapOutputId = mapOutputId;
        this.dataSizedInput = dataSizedInput;
        this.indexSizedInput = indexSizedInput;
    }

    @Override
    public int hashCode() {
        return mapOutputId.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ShuffleMapInput
                && this.mapOutputId.equals(((ShuffleMapInput) other).mapOutputId);
    }

    public MapOutputId mapOutputId() {
        return mapOutputId;
    }

    public SizedInput dataSizedInput() {
        return dataSizedInput;
    }

    public SizedInput indexSizedInput() {
        return indexSizedInput;
    }
}
