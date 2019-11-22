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

import com.palantir.spark.shuffle.async.s3.metrics.BasicShuffleClientMetrics;
import com.palantir.spark.shuffle.async.s3.metrics.MergingShuffleClientMetrics;
import com.palantir.spark.shuffle.async.s3.metrics.S3AsyncShuffleMetrics;
import com.palantir.spark.shuffle.async.s3.metrics.S3FetcherIteratorMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Slf4jS3AsyncShuffleMetrics implements S3AsyncShuffleMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(Slf4jS3AsyncShuffleMetrics.class);

    private final String sparkAppName;
    private final Slf4jBasicShuffleClientMetrics basicShuffleClientMetrics;
    private final Slf4jMergingShuffleClientMetrics mergingShuffleClientMetrics;
    private final Slf4jS3FetcherIteratorMetrics s3FetcherIteratorMetrics;

    public Slf4jS3AsyncShuffleMetrics(String sparkAppName) {
        this.sparkAppName = sparkAppName;
        this.basicShuffleClientMetrics = new Slf4jBasicShuffleClientMetrics(sparkAppName);
        this.mergingShuffleClientMetrics = new Slf4jMergingShuffleClientMetrics(sparkAppName);
        this.s3FetcherIteratorMetrics = new Slf4jS3FetcherIteratorMetrics(sparkAppName);
    }

    @Override
    public void markUsingAsyncShuffleUploadPlugin() {
        LOGGER.info("Using the async shuffle upload plugin.", Args.sparkAppNameArg(sparkAppName));
    }

    @Override
    public BasicShuffleClientMetrics basicShuffleClientMetrics() {
        return basicShuffleClientMetrics;
    }

    @Override
    public MergingShuffleClientMetrics mergingShuffleClientMetrics() {
        return mergingShuffleClientMetrics;
    }

    @Override
    public S3FetcherIteratorMetrics s3FetcherIteratorMetrics() {
        return s3FetcherIteratorMetrics;
    }
}
