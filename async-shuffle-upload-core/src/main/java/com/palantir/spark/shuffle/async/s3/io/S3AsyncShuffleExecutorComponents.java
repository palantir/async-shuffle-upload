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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.palantir.logsafe.SafeArg;
import com.palantir.spark.shuffle.async.api.SparkShuffleApiConstants;
import com.palantir.spark.shuffle.async.s3.client.BaseHadoopShuffleClientConfiguration;
import com.palantir.spark.shuffle.async.s3.client.ShuffleClient;
import com.palantir.spark.shuffle.async.s3.client.ShuffleClients;
import com.palantir.spark.shuffle.async.s3.metrics.S3AsyncShuffleMetrics;
import com.palantir.spark.shuffle.async.s3.metrics.S3AsyncShuffleMetricsFactory;
import com.palantir.spark.shuffle.async.s3.metrics.slf4j.Slf4jS3AsyncShuffleMetricsFactory;
import com.palantir.spark.shuffle.async.s3.util.NamedExecutors;
import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.config.package$;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleDataIoSparkConfigs;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleUploadDriverEndpoint;
import org.apache.spark.palantir.shuffle.async.CachingShuffleDriverEndpointRef;
import org.apache.spark.palantir.shuffle.async.JavaShuffleDriverEndpointRef;
import org.apache.spark.palantir.shuffle.async.JavaSparkConf;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.ShuffleBlockInfo;
import org.apache.spark.shuffle.api.ShuffleBlockInputStream;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.util.RpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.compat.java8.OptionConverters;

public final class S3AsyncShuffleExecutorComponents implements ShuffleExecutorComponents {

    private static final Logger LOG = LoggerFactory.getLogger(S3AsyncShuffleExecutorComponents.class);

    private final ShuffleExecutorComponents delegate;
    private final SparkConf sparkConf;
    private final ExecutorService enqueuerExecutorService;
    private final ScheduledExecutorService timeoutExecutorService;
    private final Optional<Clock> customClock;
    private final Optional<ExecutorService> customUploadExecutorService;
    private final Optional<ExecutorService> customDownloadExecutorService;
    private final Optional<ScheduledExecutorService> customUploadCoordinatorExecutorService;
    private final Supplier<SparkEnv> sparkEnvSupplier;
    private final Supplier<ShuffleFileLocator> shuffleFileLocatorSupplier;
    private final Supplier<CompressionCodec> compressionCodecSupplier;
    private final Supplier<S3AsyncShuffleMetricsFactory> metrics;
    private final int fileBufferSize;

    private SerializerManager serializerManager;
    private Optional<ShuffleClient> maybeClient;
    private boolean shouldCompressShuffle;

    // Read support is split off primarily to reduce the number of lines in the class.
    private Optional<S3AsyncShuffleReadSupport> maybeReadSupport;

    public S3AsyncShuffleExecutorComponents(
            SparkConf sparkConf,
            ShuffleExecutorComponents delegate) {
        this(
                sparkConf,
                delegate,
                Optional.empty(),
                SparkEnv::get,
                Suppliers.memoize(() -> new DefaultShuffleFileLocator(
                        new IndexShuffleBlockResolver(sparkConf, SparkEnv.get().blockManager()))),
                Suppliers.memoize(() -> CompressionCodec$.MODULE$.createCodec(sparkConf)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @VisibleForTesting
    S3AsyncShuffleExecutorComponents(
            SparkConf sparkConf,
            ShuffleExecutorComponents delegate,
            Optional<Clock> customClock,
            Supplier<SparkEnv> sparkEnvSupplier,
            Supplier<ShuffleFileLocator> shuffleFileLocatorSupplier,
            Supplier<CompressionCodec> compressionCodecSupplier,
            Optional<ExecutorService> customUploadExecutorService,
            Optional<ExecutorService> customDownloadExecutorService,
            Optional<ScheduledExecutorService> customUploadCoordinatorExecutorService,
            Optional<Supplier<S3AsyncShuffleMetricsFactory>> metrics) {
        this.sparkConf = sparkConf;
        this.delegate = delegate;
        this.enqueuerExecutorService =
                NamedExecutors.newFixedThreadExecutorService(
                        5,
                        "async-plugin-enqueuer-%d");
        this.timeoutExecutorService =
                NamedExecutors.newFixedThreadScheduledExecutorService(
                        5,
                        "async-plugin-read-timeout-%d");
        this.customClock = customClock;
        this.sparkEnvSupplier = sparkEnvSupplier;
        this.shuffleFileLocatorSupplier = shuffleFileLocatorSupplier;
        this.compressionCodecSupplier = compressionCodecSupplier;
        this.customUploadExecutorService = customUploadExecutorService;
        this.customDownloadExecutorService = customDownloadExecutorService;
        this.customUploadCoordinatorExecutorService = customUploadCoordinatorExecutorService;
        this.fileBufferSize = Math.toIntExact(new JavaSparkConf(sparkConf).getLong(
                AsyncShuffleDataIoSparkConfigs.LOCAL_FILE_STREAM_BUFFER_SIZE()));
        this.metrics = metrics.orElseGet(() -> this::loadMetricsFactory);
    }

    @Override
    public void initializeExecutor(
            String execAppId, String execId, Map<String, String> extraConfigs) {
        delegate.initializeExecutor(execAppId, execId, extraConfigs);
        SparkEnv sparkEnv = sparkEnvSupplier.get();
        BaseHadoopShuffleClientConfiguration shuffleClientConf = BaseHadoopShuffleClientConfiguration.of(sparkConf);
        Clock resolvedClock = customClock.orElseGet(Clock::systemUTC);
        ShuffleDriverEndpointRef driverEndpointRef = createDriverEndpointRef(
                sparkEnv, shuffleClientConf, resolvedClock);
        String appName = getShufflePluginAppName(sparkConf).orElse(execAppId);
        S3AsyncShuffleMetrics resolvedMetrics = metrics.get().create(sparkConf, appName);
        this.serializerManager = sparkEnv.serializerManager();
        this.shouldCompressShuffle = (boolean) sparkConf.get(package$.MODULE$.SHUFFLE_COMPRESS());
        this.maybeClient = ShuffleClients.builder()
                .appId(execAppId)
                .clock(resolvedClock)
                .driverEndpointRef(driverEndpointRef)
                .customDownloadExecutorService(customDownloadExecutorService)
                .customUploadExecutorService(customUploadExecutorService)
                .customUploadCoordinatorExecutorService(customUploadCoordinatorExecutorService)
                .sparkConf(sparkConf)
                .metrics(resolvedMetrics)
                .build();
        resolvedMetrics.markUsingAsyncShuffleUploadPlugin();
        this.maybeReadSupport = maybeClient.map(client ->
                new S3AsyncShuffleReadSupport(
                        delegate,
                        client,
                        serializerManager,
                        compressionCodecSupplier.get(),
                        shouldCompressShuffle,
                        enqueuerExecutorService,
                        timeoutExecutorService,
                        resolvedMetrics.s3FetcherIteratorMetrics(),
                        () -> Optional.ofNullable(TaskContext.get()),
                        driverEndpointRef));
    }

    @Override
    public ShuffleMapOutputWriter createMapOutputWriter(
            int shuffleId, int mapId, long mapTaskAttemptId, int numPartitions) throws IOException {
        LOG.debug("Created MapOutputWriter for shuffle partition with numPartitions",
                SafeArg.of("shuffleId", shuffleId),
                SafeArg.of("mapId", mapId),
                SafeArg.of("attemptId", mapTaskAttemptId),
                SafeArg.of("numPartitions", numPartitions));
        ShuffleMapOutputWriter delegateWriter = delegate.createMapOutputWriter(
                shuffleId, mapId, mapTaskAttemptId, numPartitions);
        return maybeClient.<ShuffleMapOutputWriter>map(client ->
                new S3AsyncShuffleMapOutputWriter(
                        delegateWriter,
                        client,
                        shuffleFileLocatorSupplier.get(),
                        shuffleId,
                        mapId,
                        mapTaskAttemptId,
                        fileBufferSize))
                .orElse(delegateWriter);
    }

    @Override
    public Iterable<ShuffleBlockInputStream> getPartitionReaders(
            Iterable<ShuffleBlockInfo> blockMetadata) throws IOException {
        if (maybeReadSupport.isPresent()) {
            return maybeReadSupport.get().getPartitionReaders(blockMetadata);
        } else {
            return delegate.getPartitionReaders(blockMetadata);
        }
    }

    @Override
    public boolean shouldWrapPartitionReaderStream() {
        // Both of these return false, but still check for presence to match pattern-wise and to future-proof against
        // changes in the underlying local disk impl
        if (maybeClient.isPresent()) {
            return false;
        } else {
            return delegate.shouldWrapPartitionReaderStream();
        }
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

    private S3AsyncShuffleMetricsFactory loadMetricsFactory() {
        String factoryClassName = sparkConf.get(AsyncShuffleDataIoSparkConfigs.METRICS_FACTORY_CLASS());
        try {
            return Class.forName(factoryClassName)
                    .asSubclass(S3AsyncShuffleMetricsFactory.class)
                    .getDeclaredConstructor()
                    .newInstance();
        } catch (Exception e) {
            LOG.error("Failed to load async shuffle plugin's metrics factory specified by the Spark configuration."
                            + " Falling back to SLF4J-logging based implementation.",
                    SafeArg.of("confKey", SparkShuffleApiConstants.METRICS_FACTORY_CLASS_CONF),
                    SafeArg.of("metricsFactoryClass", factoryClassName),
                    e);
            return new Slf4jS3AsyncShuffleMetricsFactory();
        }
    }

    private static Optional<String> getShufflePluginAppName(SparkConf sparkConf) {
        return OptionConverters.toJava(sparkConf.getOption(SparkShuffleApiConstants.SHUFFLE_PLUGIN_APP_NAME_CONF));
    }
}
