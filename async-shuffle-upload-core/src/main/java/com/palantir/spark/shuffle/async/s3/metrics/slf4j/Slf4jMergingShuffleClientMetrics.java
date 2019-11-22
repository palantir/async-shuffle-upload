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
import com.palantir.spark.shuffle.async.s3.metrics.MergingShuffleClientMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Slf4jMergingShuffleClientMetrics implements MergingShuffleClientMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(Slf4jMergingShuffleClientMetrics.class);

    private final SafeArg<String> sparkAppNameArg;

    public Slf4jMergingShuffleClientMetrics(String sparkAppName) {
        sparkAppNameArg = Args.sparkAppNameArg(sparkAppName);
    }

    @Override
    public void markDownloadRequested(int shuffleId, int mapId, long attemptId, long mergeId, String type) {
        LOGGER.info("Requested download of merged shuffle file.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                Args.mapIdArg(mapId),
                Args.attemptIdArg(attemptId),
                mergeIdArg(mergeId),
                fileTypeArg(type));
    }

    @Override
    public void markDownloadStarted(int shuffleId, int mapId, long attemptId, long mergeId, String type) {
        LOGGER.info("Started download of merged shuffle file.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                Args.mapIdArg(mapId),
                Args.attemptIdArg(attemptId),
                mergeIdArg(mergeId),
                fileTypeArg(type));
    }

    @Override
    public void markDownloadFailed(int shuffleId, int mapId, long attemptId, long mergeId, String type) {
        LOGGER.info("Failed download of merged shuffle file.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                Args.mapIdArg(mapId),
                Args.attemptIdArg(attemptId),
                mergeIdArg(mergeId),
                fileTypeArg(type));
    }

    @Override
    public void markDownloadCompleted(
            int shuffleId, int mapId, long attemptId, long mergeId, String type, long durationMillis) {
        LOGGER.info("Finished download of merged shuffle file.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                Args.mapIdArg(mapId),
                Args.attemptIdArg(attemptId),
                mergeIdArg(mergeId),
                fileTypeArg(type),
                Args.durationMillisArg(durationMillis));
    }

    @Override
    public void markUploadFailed(int shuffleId, long mergeId, String type) {
        LOGGER.info("Failed to upload merged shuffle file.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                mergeIdArg(mergeId),
                fileTypeArg(type));
    }

    @Override
    public void markUploadCompleted(
            int shuffleId, long mergeId, String type, long batchSizeBytes, long durationMillis, long latencyMillis) {
        LOGGER.info("Successfully uploaded merged shuffle file.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                mergeIdArg(mergeId),
                fileTypeArg(type),
                SafeArg.of("batchSizeBytes", batchSizeBytes),
                Args.durationMillisArg(durationMillis),
                Args.latencyMillisArg(latencyMillis));
    }

    @Override
    public void markUploadStarted(int shuffleId, long mergeId, String type) {
        LOGGER.info("Started merged shuffle file upload.",
                sparkAppNameArg,
                Args.shuffleIdArg(shuffleId),
                mergeIdArg(mergeId),
                fileTypeArg(type));
    }

    private static SafeArg<Long> mergeIdArg(long mergeId) {
        return SafeArg.of("mergeId", mergeId);
    }

    private static SafeArg<String> fileTypeArg(String fileType) {
        return SafeArg.of("fileType", fileType);
    }
}
