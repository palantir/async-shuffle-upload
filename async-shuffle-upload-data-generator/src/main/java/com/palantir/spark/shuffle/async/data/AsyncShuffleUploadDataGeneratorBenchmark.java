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

package com.palantir.spark.shuffle.async.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.spark.shuffle.async.data.config.BenchmarkRunConfig;
import com.palantir.spark.shuffle.async.data.config.DataGeneratorConfig;
import com.palantir.spark.shuffle.async.data.config.HadoopAsyncUploadConfig;
import com.palantir.spark.shuffle.async.s3.client.AsyncShuffleBenchmark;
import com.palantir.spark.shuffle.async.s3.client.BaseHadoopShuffleClientConfiguration;
import com.palantir.spark.shuffle.async.s3.client.ShuffleClient;
import com.palantir.spark.shuffle.async.s3.client.ShuffleClients;
import com.palantir.spark.shuffle.async.s3.client.ShuffleStorageStrategy;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.concurrent.ExecutionException;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleDataIoSparkConfigs;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleUploadDriverEndpoint;
import org.apache.spark.palantir.shuffle.async.CachingShuffleDriverEndpointRef;
import org.apache.spark.palantir.shuffle.async.JavaShuffleDriverEndpointRef;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;
import org.apache.spark.util.RpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AsyncShuffleUploadDataGeneratorBenchmark {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncShuffleUploadDataGeneratorBenchmark.class);

    private static final Path DEFAULT_CONFIG_FILE = Paths.get("var", "conf", "config.yml");

    private static final ObjectMapper YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private AsyncShuffleUploadDataGeneratorBenchmark() {}

    @SuppressWarnings("Slf4jConstantLogMessage")
    public static void main(String[] _args) throws IOException, ExecutionException, InterruptedException {
        File configFile = new File(DEFAULT_CONFIG_FILE.toString());
        DataGeneratorConfig config;
        HadoopAsyncUploadConfig hadoopConfig;
        if (configFile.exists()) {
            BenchmarkRunConfig yamlConfig = YAML_OBJECT_MAPPER
                    .readValue(configFile, BenchmarkRunConfig.class);
            config = yamlConfig.dataGeneratorConfig();
            hadoopConfig = yamlConfig.hadoopAsyncUploadConfig();
        } else {
            // load from environment variables
            config = DataGeneratorConfig.builder()
                    .partitionSizeMb(getIntFromEnv("ASYNC_DATA_GENERATOR_PARTITION_SIZE_MB"))
                    .shuffleSizeMb(getIntFromEnv("ASYNC_DATA_GENERATOR_SHUFFLE_SIZE_MB"))
                    .totalTestSizeMb(getIntFromEnv("ASYNC_DATA_GENERATOR_TOTAL_SIZE_MB"))
                    .readers(getIntFromEnv("ASYNC_DATA_GENERATOR_NUM_READERS"))
                    .writers(getIntFromEnv("ASYNC_DATA_GENERATOR_NUM_WRITERS"))
                    .build();

            hadoopConfig = HadoopAsyncUploadConfig.builder()
                    .basePath(System.getenv("ASYNC_DATA_GENERATOR_BASE_PATH"))
                    .accessKeyId(System.getenv("ASYNC_DATA_GENERATOR_AWS_ACCESS_KEY"))
                    .secretKey(System.getenv("ASYNC_DATA_GENERATOR_AWS_SECRET_KEY"))
                    .build();
        }

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .set(AsyncShuffleDataIoSparkConfigs.BASE_URI(), hadoopConfig.basePath())
                .set(AsyncShuffleDataIoSparkConfigs.UPLOAD_PARALLELISM(), config.writers())
                .set(AsyncShuffleDataIoSparkConfigs.STORAGE_STRATEGY(), ShuffleStorageStrategy.BASIC.value())
                .set(AsyncShuffleDataIoSparkConfigs.DOWNLOAD_PARALLELISM(), config.readers());

        // Boot up spark context so we have a handle on a SparkEnv
        SparkContext sc = SparkContext.getOrCreate(sparkConf);
        BaseHadoopShuffleClientConfiguration clientConf = BaseHadoopShuffleClientConfiguration.of(sparkConf);
        ShuffleClient client = ShuffleClients.builder()
                .appId(sc.applicationId())
                .sparkConf(sparkConf)
                .customUploadExecutorService(MoreExecutors.newDirectExecutorService())
                .customDownloadExecutorService(MoreExecutors.newDirectExecutorService())
                .driverEndpointRef(createDriverEndpointRef(SparkEnv.get(), clientConf, Clock.systemUTC()))
                .build()
                .get();

        AsyncShuffleBenchmark benchmark = new AsyncShuffleBenchmark(config, client);
        double writeThroughput = benchmark.runWrite();
        double readThroughput = benchmark.runRead();

        AsyncShuffleUploadResults results = AsyncShuffleUploadResults.builder()
                .baseUri(System.getenv("ASYNC_DATA_GENERATOR_BASE_PATH"))
                .config(config)
                .readThroughputBytesPerSec(readThroughput)
                .writeThroughputBytesPerSec(writeThroughput)
                .build();

        String resultsString = OBJECT_MAPPER.writeValueAsString(results);
        LOGGER.info("Results:");
        LOGGER.info(resultsString);

        LOGGER.info("Deleting shuffle data");
        LOGGER.info("Test complete");
    }

    private static int getIntFromEnv(String name) {
        String value = System.getenv(name);
        Preconditions.checkNotNull(value, "Expected env variable to not be null",
                SafeArg.of("envVar", name));
        return Integer.valueOf(value);
    }

    private static ShuffleDriverEndpointRef createDriverEndpointRef(
            SparkEnv sparkEnv, BaseHadoopShuffleClientConfiguration baseConfig, Clock clock) {
        return new CachingShuffleDriverEndpointRef(
                new JavaShuffleDriverEndpointRef(
                        RpcUtils.makeDriverRef(
                                AsyncShuffleUploadDriverEndpoint.NAME(),
                                baseConfig.sparkConf(),
                                sparkEnv.rpcEnv())),
                baseConfig.driverRefCacheExpirationMillis(),
                baseConfig.driverRefCacheSize(),
                clock);
    }
}
