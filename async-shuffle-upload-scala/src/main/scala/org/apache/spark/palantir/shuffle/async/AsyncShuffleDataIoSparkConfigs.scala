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

package org.apache.spark.palantir.shuffle.async

import java.util.concurrent.TimeUnit

import com.palantir.spark.shuffle.async.api.SparkShuffleApiConstants
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit

import scala.util.Try

object AsyncShuffleDataIoSparkConfigs {
  val BASE_URI = ConfigBuilder(SparkShuffleApiConstants.SHUFFLE_BASE_URI_CONF)
    .doc("Base URI for the backup files. Should be able to be loaded by a Hadoop file system.")
    .stringConf
    .createOptional

  val S3A_ENDPOINT = ConfigBuilder(SparkShuffleApiConstants.SHUFFLE_S3A_ENDPOINT_CONF)
    .doc("If the shuffle client is configured for s3a, this specifies the s3a endpoint")
    .stringConf
    .createOptional

  val DEFAULT_DOWNLOAD_PARALLELISM = 10

  val DOWNLOAD_PARALLELISM = ConfigBuilder("spark.shuffle.hadoop.async.download.parallelism")
    .doc("Number of threads in the thread pool that download backup shuffle files.")
    .intConf
    .createWithDefault(DEFAULT_DOWNLOAD_PARALLELISM)


  val DEFAULT_UPLOAD_PARALLELISM = 5

  val UPLOAD_PARALLELISM = ConfigBuilder("spark.shuffle.hadoop.async.upload.parallelism")
    .doc("Number of threads in the thread pool that uploads backup shuffle files.")
    .intConf
    .createWithDefault(DEFAULT_UPLOAD_PARALLELISM)

  val STORAGE_STRATEGY = ConfigBuilder("spark.shuffle.hadoop.async.storage.strategy")
    .doc("Strategy to use for uploading shuffle files to the backing store.")
    .stringConf
    .checkValues(Set("basic", "merging"))
    .createWithDefault("basic")

  val DEFAULT_READ_LOCAL_DISK_PARALLELISM = 5

  val READ_LOCAL_DISK_PARALLELISM = ConfigBuilder("spark.shuffle.hadoop.async.storage.merging.local.read.parallelism")
    .doc("When using the \"merging\" storage strategy, files are downloaded to local disk before map output data is" +
      " shipped back to the shuffle reader. This configuration controls the parallelism for the local disk reads" +
      " after files are downloaded.")
    .intConf
    .createWithDefault(DEFAULT_READ_LOCAL_DISK_PARALLELISM)

  // Default merged batch size of 10MB
  val DEFAULT_MERGING_STORAGE_BATCH_SIZE_BYTES = 10 * 1024 * 1024

  val MERGED_BATCH_SIZE = ConfigBuilder("spark.shuffle.hadoop.async.storage.merging.batch.size")
    .doc("Approximate target size for the combined shuffle files when using the merging storage strategy. The merging" +
      " storage strategy combines shuffle files before uploading them to the backing store - so this configuration" +
      " sets a target size for the combined files before they are uploaded. Note that large shuffle files may cause" +
      " the uploaded blob to exceed this threshold.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(DEFAULT_MERGING_STORAGE_BATCH_SIZE_BYTES)

  val DEFAULT_MERGING_MAX_BUFFERED_INPUTS = 2048

  val MERGING_MAX_BUFFERED_INPUTS = ConfigBuilder("spark.shuffle.hadoop.async.storage.merging.buffer.max")
    .doc("Number of inputs that can be buffered on the executor before the shuffle uploader forces all files to be" +
      " uploaded to the backing store. This is to prevent the merging buffer from growing without bound.")
    .intConf
    .createWithDefault(DEFAULT_MERGING_MAX_BUFFERED_INPUTS)

  // 5 second default uploading polling period
  val DEFAULT_UPLOAD_POLLING_PERIOD_MILLIS = 5 * 1000

  val UPLOAD_POLLING_PERIOD = ConfigBuilder("spark.shuffle.hadoop.async.storage.merging.upload.period")
    .doc("Interval at which the executors will check for uploading shuffle data.")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefault(DEFAULT_UPLOAD_POLLING_PERIOD_MILLIS)

  val DEFAULT_MERGED_BATCH_MAXIMUM_BUFFERED_AGE_MINUTES = 5

  val MERGED_BATCH_MAXIMUM_BUFFERED_AGE = ConfigBuilder("spark.shuffle.hadoop.async.storage.merging.buffer.age")
    .doc("Amount of time the executor will wait before uploading all the files for a batch.")
    .timeConf(TimeUnit.MINUTES)
    .createWithDefault(DEFAULT_MERGED_BATCH_MAXIMUM_BUFFERED_AGE_MINUTES)

  val DEFAULT_STREAM_BUFFER_SIZE_BYTES = 128 * 1024

  // TODO(mcheah): #25 Introduce more configurations for individual buffer sizes for different buffer purposes
  val STREAM_BUFFER_SIZE = ConfigBuilder("spark.shuffle.hadoop.async.io.buffer.size")
    .doc("Size of buffers when reading and writing data from storage, both remote and on local disk.")
    .bytesConf(ByteUnit.BYTE)
    .checkValue(validator = bytes => {
      Try {
        Math.toIntExact(bytes)
        true
      } getOrElse false
    }, errorMsg = s"Buffer size must be less than ${Integer.MAX_VALUE}")
    .createWithDefault(DEFAULT_STREAM_BUFFER_SIZE_BYTES)

  val LOCAL_FILE_STREAM_BUFFER_SIZE = ConfigBuilder("spark.shuffle.hadoop.async.io.disk.buffer.size")
    .doc("Size of buffers when reading and writing data on local disk")
    .fallbackConf(STREAM_BUFFER_SIZE)

  val REMOTE_FILE_STREAM_BUFFER_SIZE = ConfigBuilder("spark.shuffle.hadoop.async.io.network.buffer.size")
    .doc("Size of buffers when reading and writing data over the network (e.g. S3)")
    .fallbackConf(STREAM_BUFFER_SIZE)

  val DEFAULT_DRIVER_REF_CACHE_MAX_SIZE = 1024

  val DRIVER_REF_CACHE_MAX_SIZE = ConfigBuilder("spark.shuffle.hadoop.async.driverref.cache.size")
    .doc("Maximum size of the cache used by each executor to avoid calling the driver in excess amounts.")
    .intConf
    .createWithDefault(DEFAULT_DRIVER_REF_CACHE_MAX_SIZE)

  // Default of 1 minute for the driver ref cache's expiration period.
  val DEFAULT_DRIVER_REF_CACHE_EXPIRATION_PERIOD_MILLIS = 60000

  val DRIVER_REF_CACHE_EXPIRATION_PERIOD = ConfigBuilder("spark.shuffle.hadoop.driverref.cache.expiration.period")
    .doc("Duration results from communicating with the driver will be cached on the executor before being" +
      " invalidated.")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefault(DEFAULT_DRIVER_REF_CACHE_EXPIRATION_PERIOD_MILLIS)

  val METRICS_FACTORY_CLASS = ConfigBuilder(SparkShuffleApiConstants.METRICS_FACTORY_CLASS_CONF)
    .doc("Implementation of the metrics system when using the plugin.")
    .stringConf
    .createWithDefault(
      "com.palantir.spark.shuffle.async.s3.metrics.slf4j.Slf4jS3AsyncShuffleMetricsFactory")
}
