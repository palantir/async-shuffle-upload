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

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.spark.shuffle.async.s3.util.SizedInput;
import java.io.InputStream;

public interface ShuffleClient {

    void asyncWriteIndexFileAndClose(
            SizedInput indexSizedInput,
            int shuffleId,
            int mapId,
            long attemptId);

    void asyncWriteDataAndIndexFilesAndClose(
            SizedInput dataSizedInput,
            SizedInput indexSizedInput,
            int shuffleId,
            int mapId,
            long attemptId);

    ListenableFuture<InputStream> getBlockData(
            int shuffleId,
            int mapId,
            int reduceId,
            long attemptId);

    void removeApplicationData();
}
