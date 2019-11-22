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

import com.palantir.logsafe.SafeArg;
import com.palantir.spark.shuffle.async.s3.metadata.BlacklistedExecutorTracker;
import com.palantir.spark.shuffle.async.s3.metadata.ShuffleIdTracker;
import java.io.IOException;
import java.util.Map;
import org.apache.spark.SparkEnv;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleUploadDriverEndpoint;
import org.apache.spark.rpc.RpcEndpoint;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class S3AsyncShuffleDriverComponents implements ShuffleDriverComponents {

    private static final Logger LOG = LoggerFactory.getLogger(S3AsyncShuffleDriverComponents.class);

    private final ShuffleDriverComponents delegate;
    private final ShuffleIdTracker shuffleIdTracker;
    private final BlacklistedExecutorTracker blacklistedExecutorTracker;

    private RpcEndpoint shuffleUploadDriverEndpoint;

    public S3AsyncShuffleDriverComponents(
            ShuffleDriverComponents delegate,
            ShuffleIdTracker shuffleIdTracker,
            BlacklistedExecutorTracker blacklistedExecutorTracker) {
        this.delegate = delegate;
        this.shuffleIdTracker = shuffleIdTracker;
        this.blacklistedExecutorTracker = blacklistedExecutorTracker;
    }

    @Override
    public Map<String, String> initializeApplication() {
        RpcEnv sparkRpcEnv = SparkEnv.get().rpcEnv();
        shuffleUploadDriverEndpoint = new AsyncShuffleUploadDriverEndpoint(
                sparkRpcEnv,
                shuffleIdTracker,
                blacklistedExecutorTracker);
        sparkRpcEnv.setupEndpoint(AsyncShuffleUploadDriverEndpoint.NAME(), shuffleUploadDriverEndpoint);
        return delegate.initializeApplication();
    }

    @Override
    public void cleanupApplication() throws IOException {
        LOG.info("Cleaning up application data");
        shuffleUploadDriverEndpoint.stop();
        delegate.cleanupApplication();
    }

    @Override
    public void registerShuffle(int shuffleId) throws IOException {
        shuffleIdTracker.registerShuffle(shuffleId);
        delegate.registerShuffle(shuffleId);
    }

    @Override
    public void removeShuffle(int shuffleId, boolean blocking) throws IOException {
        LOG.info("Cleaning up shuffle data ", SafeArg.of("shuffleId", shuffleId));
        shuffleIdTracker.unregisterShuffle(shuffleId);
        delegate.removeShuffle(shuffleId, blocking);
    }

    @Override
    public boolean checkIfMapOutputStoredOutsideExecutor(int shuffleId, int mapId, long mapTaskAttemptId) {
        return shuffleIdTracker.isMapOutputRegistered(shuffleId, mapId, mapTaskAttemptId);
    }

    @Override
    public boolean unregisterOutputOnHostOnFetchFailure() {
        return delegate.unregisterOutputOnHostOnFetchFailure();
    }
}
