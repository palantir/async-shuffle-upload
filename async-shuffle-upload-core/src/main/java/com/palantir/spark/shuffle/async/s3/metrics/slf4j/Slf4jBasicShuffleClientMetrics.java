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
import com.palantir.spark.shuffle.async.s3.metrics.BasicShuffleClientMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Slf4jBasicShuffleClientMetrics implements BasicShuffleClientMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(Slf4jBasicShuffleClientMetrics.class);

    private final SafeArg<String> sparkAppNameArg;

    public Slf4jBasicShuffleClientMetrics(String sparkAppName) {
        this.sparkAppNameArg = Args.sparkAppNameArg(sparkAppName);
    }

    @Override
    public void markDownloadRequested(int shuffleId, int mapId, int reduceId, long attemptId) {
        LOGGER.info("Requested to download shuffle block from remote storage.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                Args.mapIdArg(mapId),
                Args.reduceIdArg(reduceId),
                Args.attemptIdArg(attemptId));
    }

    @Override
    public void markDownloadStarted(int shuffleId, int mapId, int reduceId, long attemptId) {
        LOGGER.info("Started downloading shuffle block from remote storage.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                Args.mapIdArg(mapId),
                Args.reduceIdArg(reduceId),
                Args.attemptIdArg(attemptId));
    }

    @Override
    public void markUploadRequested(int shuffleId, int mapId, long attemptId, long numRunningOrPendingUploads) {
        LOGGER.info("Requested to upload map output file.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                Args.mapIdArg(mapId),
                Args.attemptIdArg(attemptId),
                numRunningOrPendingUploadsArg(numRunningOrPendingUploads));
    }

    @Override
    public void markUploadRequestSubmitted(
            int shuffleId,
            int mapId,
            long attemptId,
            long requestSubmissionLatencyMillis) {
        LOGGER.info("Requested to upload map output file was submitted to the thread pool.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                Args.mapIdArg(mapId),
                Args.attemptIdArg(attemptId),
                SafeArg.of("requestSubmissionLatencyMillis", requestSubmissionLatencyMillis));
    }

    @Override
    public void markUploadStarted(int shuffleId, int mapId, long attemptId) {
        LOGGER.info("Beginning to upload shuffle file.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                Args.mapIdArg(mapId),
                Args.attemptIdArg(attemptId));
    }

    @Override
    public void markUploadFailed(int shuffleId, int mapId, long attemptId, long numRunningOrPendingUploads) {
        LOGGER.info("Failed to upload shuffle file.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                Args.mapIdArg(mapId),
                Args.attemptIdArg(attemptId),
                numRunningOrPendingUploadsArg(numRunningOrPendingUploads));
    }

    @Override
    public void markUploadCompleted(
            int shuffleId,
            int mapId,
            long attemptId,
            long durationMillis,
            long bytesUploaded,
            long latencyMillis,
            long numRunningOrPendingUploads) {
        LOGGER.info("Finished uploading shuffle map output file.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                Args.mapIdArg(mapId),
                Args.attemptIdArg(attemptId),
                Args.durationMillisArg(durationMillis),
                SafeArg.of("bytesUploaded", bytesUploaded),
                Args.latencyMillisArg(latencyMillis),
                numRunningOrPendingUploadsArg(numRunningOrPendingUploads));
    }

    private static SafeArg<Long> numRunningOrPendingUploadsArg(long numRunningOrPendingUploads) {
        return SafeArg.of("numRunningOrPendingUploads", numRunningOrPendingUploads);
    }
}
