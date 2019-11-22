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

package com.palantir.spark.shuffle.async.s3.metrics.slf4j;

import com.palantir.logsafe.SafeArg;

final class Args {

    static SafeArg<String> sparkAppNameArg(String sparkAppName) {
        return SafeArg.of("sparkAppName", sparkAppName);
    }

    static SafeArg<Integer> shuffleIdArg(int shuffleId) {
        return SafeArg.of("shuffleId", shuffleId);
    }

    static SafeArg<Integer> mapIdArg(int mapId) {
        return SafeArg.of("mapId", mapId);
    }

    static SafeArg<Integer> reduceIdArg(int reduceId) {
        return SafeArg.of("reduceId", reduceId);
    }

    static SafeArg<Long> attemptIdArg(long attemptId) {
        return SafeArg.of("mapTaskAttemptId", attemptId);
    }

    static SafeArg<Long> durationMillisArg(long durationMillis) {
        return SafeArg.of("durationMillis", durationMillis);
    }

    static SafeArg<Long> latencyMillisArg(long latencyMillis) {
        return SafeArg.of("latencyMillis", latencyMillis);
    }

    private Args() {}
}
