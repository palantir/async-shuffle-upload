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

package com.palantir.spark.shuffle.async.s3.metrics;

import com.palantir.spark.shuffle.async.api.SparkShuffleApiConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleDataIoSparkConfigs;

/**
 * Entry point for collecting metrics from shuffle uploads.
 * <p>
 * Implementations are instantiated reflectively with a no-arg constructor. Specify the implementation class via
 * {@link AsyncShuffleDataIoSparkConfigs#METRICS_FACTORY_CLASS()}.
 */
public interface S3AsyncShuffleMetricsFactory {

    /**
     * Instantiate a metrics system for measuring the performance of shuffle uploads and downloads.
     * <p>
     * @param sparkConf The Spark configuration of the application.
     * @param sparkAppName The name of the application. Either the appId, or can be overridden by
     *                     {@link SparkShuffleApiConstants#SHUFFLE_PLUGIN_APP_NAME_CONF} to give a custom tag for
     *                     metrics that are collected by the system.
     */
    S3AsyncShuffleMetrics create(SparkConf sparkConf, String sparkAppName);
}
