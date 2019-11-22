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

import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.spark.shuffle.async.api.SparkShuffleApiConstants;
import com.palantir.spark.shuffle.async.s3.client.basic.HadoopShuffleClient;
import com.palantir.spark.shuffle.async.s3.client.merging.DefaultMergingShuffleFiles;
import com.palantir.spark.shuffle.async.s3.client.merging.DefaultShuffleFileBatchUploader;
import com.palantir.spark.shuffle.async.s3.client.merging.MergingHadoopShuffleClientConfiguration;
import com.palantir.spark.shuffle.async.s3.client.merging.MergingShuffleFiles;
import com.palantir.spark.shuffle.async.s3.client.merging.MergingShuffleUploadCoordinator;
import com.palantir.spark.shuffle.async.s3.client.merging.ShuffleFileBatchUploader;
import com.palantir.spark.shuffle.async.s3.metrics.S3AsyncShuffleMetrics;
import com.palantir.spark.shuffle.async.s3.util.DaemonExecutors;
import com.palantir.spark.shuffle.async.s3.util.NamedExecutors;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;
import org.immutables.builder.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.compat.java8.OptionConverters;

public final class ShuffleClients {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleClients.class);

    @Builder.Factory
    static Optional<ShuffleClient> shuffleClient(
            String appId,
            SparkConf sparkConf,
            ShuffleDriverEndpointRef driverEndpointRef,
            Clock clock,
            Optional<ExecutorService> customUploadExecutorService,
            Optional<ExecutorService> customDownloadExecutorService,
            Optional<ScheduledExecutorService> customUploadCoordinatorExecutorService,
            S3AsyncShuffleMetrics metrics) {
        BaseHadoopShuffleClientConfiguration baseConfig = BaseHadoopShuffleClientConfiguration.of(sparkConf);
        if (!baseConfig.baseUri().isPresent()) {
            LOGGER.warn("Spark application configured to use the async shuffle upload plugin, but no remote storage"
                    + " location was provided for backing up data. Falling back to only reading and writing shuffle"
                    + " files from local disk.");
        }
        return baseConfig.baseUri().map(baseUri -> {
            LOGGER.info("Setting up shuffle hadoop client.", UnsafeArg.of("baseUri", baseUri));
            ExecutorService resolvedUploadExecutor = resolveUploadExecutor(customUploadExecutorService, baseConfig);
            ExecutorService resolvedDownloadExecutor =
                    resolveDownloadExecutor(customDownloadExecutorService, baseConfig);
            FileSystem backupFs = createBackupFs(baseConfig, baseUri);

            ShuffleStorageStrategy storageStrategy = baseConfig.storageStrategy();
            switch (storageStrategy) {
                case BASIC:
                    return new HadoopShuffleClient(
                            baseUri,
                            appId,
                            MoreExecutors.listeningDecorator(resolvedUploadExecutor),
                            MoreExecutors.listeningDecorator(resolvedDownloadExecutor),
                            backupFs,
                            metrics.basicShuffleClientMetrics(),
                            driverEndpointRef,
                            baseConfig.remoteFileBufferSize(),
                            clock);
                case MERGING:
                    MergingHadoopShuffleClientConfiguration mergingConf =
                            MergingHadoopShuffleClientConfiguration.of(baseConfig);
                    MergingShuffleFiles mergingShuffleFiles = DefaultMergingShuffleFiles.mergingShuffleFiles(
                            appId,
                            baseConfig.localFileBufferSize(),
                            baseConfig.remoteFileBufferSize(),
                            baseUri,
                            backupFs);

                    ScheduledExecutorService resolvedUploadCoordinatorExecutor =
                            customUploadCoordinatorExecutorService.orElseGet(() ->
                                    NamedExecutors.newFixedThreadScheduledExecutorService(
                                            1,
                                            "upload-coordinator-%d"));
                    ShuffleFileBatchUploader batchUploader = new DefaultShuffleFileBatchUploader(
                            appId,
                            driverEndpointRef,
                            mergingShuffleFiles,
                            MoreExecutors.listeningDecorator(resolvedUploadExecutor),
                            metrics.mergingShuffleClientMetrics(),
                            clock);

                    MergingShuffleUploadCoordinator coordinator = new MergingShuffleUploadCoordinator(
                            mergingConf.maxBatchSizeBytes(),
                            mergingConf.maxBatchAgeMillis(),
                            mergingConf.maxBufferedInputs(),
                            mergingConf.pollingIntervalMillis(),
                            driverEndpointRef,
                            MoreExecutors.listeningDecorator(resolvedUploadCoordinatorExecutor),
                            clock,
                            batchUploader);

                    ExecutorService localReadExecService = DaemonExecutors.newFixedDaemonThreadPool(
                            mergingConf.readLocalDiskParallelism());
                    ShuffleClient client = new MergingHadoopShuffleClient(
                            appId,
                            MoreExecutors.listeningDecorator(resolvedDownloadExecutor),
                            MoreExecutors.listeningDecorator(localReadExecService),
                            metrics.mergingShuffleClientMetrics(),
                            driverEndpointRef,
                            coordinator,
                            mergingShuffleFiles,
                            clock);

                    coordinator.start();
                    return client;
                default:
                    throw new SafeIllegalArgumentException(
                            "Storage strategy is invalid.",
                            SafeArg.of("invalidStorageStrategy", storageStrategy));
            }
        });
    }

    private static FileSystem createBackupFs(BaseHadoopShuffleClientConfiguration baseConfig, URI baseUri) {
        FileSystem backupFs;
        try {
            backupFs = FileSystem.get(baseUri, baseConfig.hadoopConf());
        } catch (IOException e) {
            LOGGER.error("Failed to create filesystem", e);
            throw new RuntimeException(e);
        }
        return backupFs;
    }

    private static ExecutorService resolveDownloadExecutor(
            Optional<ExecutorService> customDownloadExecutorService,
            BaseHadoopShuffleClientConfiguration baseConfig) {
        return customDownloadExecutorService.orElseGet(
                () -> NamedExecutors.newFixedThreadExecutorService(
                        baseConfig.downloadParallelism(), "async-plugin-hadoop-download-%d"));
    }

    private static ExecutorService resolveUploadExecutor(
            Optional<ExecutorService> customUploadExecutorService,
            BaseHadoopShuffleClientConfiguration baseConfig) {
        return customUploadExecutorService.orElseGet(
                () -> NamedExecutors.newFixedThreadExecutorService(
                        baseConfig.uploadParallelism(), "async-plugin-hadoop-upload-%d"));
    }

    public static Optional<String> getShufflePluginAppName(SparkConf sparkConf) {
        return OptionConverters.toJava(sparkConf.getOption(SparkShuffleApiConstants.SHUFFLE_PLUGIN_APP_NAME_CONF));
    }

    public static ShuffleClientBuilder builder() {
        return new ShuffleClientBuilder();
    }

    private ShuffleClients() {}
}
