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
import com.palantir.spark.shuffle.async.s3.metrics.S3FetcherIteratorMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Slf4jS3FetcherIteratorMetrics implements S3FetcherIteratorMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(Slf4jS3FetcherIteratorMetrics.class);

    private final SafeArg<String> sparkAppNameArg;

    public Slf4jS3FetcherIteratorMetrics(String sparkAppName) {
        this.sparkAppNameArg = Args.sparkAppNameArg(sparkAppName);
    }

    @Override
    public void markFetchFromRemoteFailed(int shuffleId, int mapId, int reduceId, long attemptId) {
        LOGGER.info("Failed to fetch shuffle blocks from remote storage.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                Args.mapIdArg(mapId),
                Args.reduceIdArg(reduceId),
                Args.attemptIdArg(attemptId));
    }

    @Override
    public void markFetchFromRemoteSucceeded(int shuffleId, int mapId, int reduceId, long attemptId) {
        LOGGER.info("Successfully fetched shuffle blocks from remote storage.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                Args.mapIdArg(mapId),
                Args.reduceIdArg(reduceId),
                Args.attemptIdArg(attemptId));
    }

    @Override
    public void markFetchFromExecutorFailed(int shuffleId, int mapId, int reduceId, long attemptId) {
        LOGGER.info("Failed to fetch shuffle blocks from other executors in the application.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                Args.mapIdArg(mapId),
                Args.reduceIdArg(reduceId),
                Args.attemptIdArg(attemptId));
    }
}
