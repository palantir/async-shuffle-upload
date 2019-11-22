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

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.spark.shuffle.async.api.SparkShuffleApiConstants;
import com.palantir.spark.shuffle.async.api.SparkShuffleAwsCredentials;
import com.palantir.spark.shuffle.async.immutables.ImmutablesStyle;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil$;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleDataIoSparkConfigs;
import org.apache.spark.palantir.shuffle.async.JavaSparkConf;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.compat.java8.OptionConverters;

/**
 * Extractors for configuration specified via the SparkConf.
 * <p>
 * Hides the need to call <code>sparkConf.get(spark.reallyLongKey)</code> everywhere.
 */
@Value.Immutable
@ImmutablesStyle
public interface BaseHadoopShuffleClientConfiguration {

    Logger LOGGER = LoggerFactory.getLogger(BaseHadoopShuffleClientConfiguration.class);

    SparkConf sparkConf();

    @Value.Derived
    default Configuration hadoopConf() {
        Configuration resolvedHadoopConf = SparkHadoopUtil$.MODULE$.newConfiguration(sparkConf());
        baseUri().filter(baseUri -> baseUri.getScheme().equals("s3a")).ifPresent(baseUri -> {
            SparkShuffleAwsCredentials awsCredentials = getMandatoryAwsCredentials(sparkConf());
            resolvedHadoopConf.set("fs.s3a.access.key", awsCredentials.accessKeyId());
            resolvedHadoopConf.set("fs.s3a.secret.key", awsCredentials.secretAccessKey());
            resolvedHadoopConf.set("fs.s3a.session.token", awsCredentials.sessionToken());
            resolvedHadoopConf.set("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
            s3aEndpoint().ifPresent(s3aEndpoint -> resolvedHadoopConf.set("fs.s3a.endpoint", s3aEndpoint));
        });
        resolvedHadoopConf.setBoolean("fs.s3a.impl.disable.cache", true);
        resolvedHadoopConf.setBoolean("fs.s3.impl.disable.cache", true);
        resolvedHadoopConf.setBoolean("fs.hdfs.impl.disable.cache", true);
        resolvedHadoopConf.setBoolean("fs.file.impl.disable.cache", true);
        return resolvedHadoopConf;
    }

    static SparkShuffleAwsCredentials getMandatoryAwsCredentials(SparkConf sparkConf) {
        Optional<String> credentialsFilename =
                OptionConverters.toJava(sparkConf.getOption(SparkShuffleApiConstants.SHUFFLE_S3A_CREDS_FILE_CONF));

        Preconditions.checkNotNull(credentialsFilename, "Expected spark config to be set",
                        SafeArg.of("config", SparkShuffleApiConstants.SHUFFLE_S3A_CREDS_FILE_CONF));
        try {
            return SparkShuffleAwsCredentials.fromBytes(Files.readAllBytes(Paths.get(credentialsFilename.get())));
        } catch (FileNotFoundException e) {
            LOGGER.error("Expected aws credentials file to be there",
                    SafeArg.of("credentialsFile", credentialsFilename.get()),
                    e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Value.Derived
    default JavaSparkConf javaSparkConf() {
        return new JavaSparkConf(sparkConf());
    }

    @Value.Derived
    default Optional<URI> baseUri() {
        return OptionConverters.toJava(
                sparkConf().get(AsyncShuffleDataIoSparkConfigs.BASE_URI())).map(URI::create);
    }

    @Value.Derived
    default Optional<String> s3aEndpoint() {
        return OptionConverters.toJava(
                sparkConf().get(AsyncShuffleDataIoSparkConfigs.S3A_ENDPOINT()));
    }

    @Value.Derived
    default int localFileBufferSize() {
        return Math.toIntExact(
                javaSparkConf().getLong(AsyncShuffleDataIoSparkConfigs.LOCAL_FILE_STREAM_BUFFER_SIZE()));
    }

    @Value.Derived
    default int remoteFileBufferSize() {
        return Math.toIntExact(
                javaSparkConf().getLong(AsyncShuffleDataIoSparkConfigs.REMOTE_FILE_STREAM_BUFFER_SIZE()));
    }

    @Value.Derived
    default int downloadParallelism() {
        return javaSparkConf().getInt(
                AsyncShuffleDataIoSparkConfigs.DOWNLOAD_PARALLELISM());
    }

    @Value.Derived
    default int uploadParallelism() {
        return javaSparkConf().getInt(
                AsyncShuffleDataIoSparkConfigs.UPLOAD_PARALLELISM());
    }

    @Value.Derived
    default int driverRefCacheSize() {
        return javaSparkConf().getInt(
                AsyncShuffleDataIoSparkConfigs.DRIVER_REF_CACHE_MAX_SIZE());
    }

    @Value.Derived
    default long driverRefCacheExpirationMillis() {
        return javaSparkConf().getLong(
                AsyncShuffleDataIoSparkConfigs.DRIVER_REF_CACHE_EXPIRATION_PERIOD());
    }

    @Value.Derived
    default ShuffleStorageStrategy storageStrategy() {
        return ShuffleStorageStrategy.fromString(
                sparkConf().get(AsyncShuffleDataIoSparkConfigs.STORAGE_STRATEGY()));
    }

    static Builder builder() {
        return new Builder();
    }

    final class Builder extends ImmutableBaseHadoopShuffleClientConfiguration.Builder {}

    static BaseHadoopShuffleClientConfiguration of(SparkConf sparkConf) {
        return builder().sparkConf(sparkConf).build();
    }
}
