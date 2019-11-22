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

import com.google.common.base.Suppliers;
import com.palantir.spark.shuffle.async.s3.metadata.BlacklistedExecutorTracker;
import com.palantir.spark.shuffle.async.s3.metadata.ShuffleIdTracker;
import java.util.function.Supplier;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleDataIO;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class S3AsyncShuffleDataIo implements ShuffleDataIO {
    private static final Logger log = LoggerFactory.getLogger(S3AsyncShuffleDataIo.class);

    private final SparkConf sparkConf;
    private final LocalDiskShuffleDataIO delegate;
    private final Supplier<ShuffleDriverComponents> shuffleDriverComponentsSupplier;

    public S3AsyncShuffleDataIo(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
        this.delegate = new LocalDiskShuffleDataIO(sparkConf);
        // HACK: this is a workaround until spark changes merge so that we only call shuffleDataIO.driver()
        // in a single place
        this.shuffleDriverComponentsSupplier = Suppliers.memoize(() ->
            new S3AsyncShuffleDriverComponents(
                    delegate.driver(),
                    new ShuffleIdTracker(),
                    new BlacklistedExecutorTracker()));
    }

    @Override
    public ShuffleDriverComponents driver() {
        log.info("Initializing shuffle driver");
        return shuffleDriverComponentsSupplier.get();
    }

    @Override
    public ShuffleExecutorComponents executor() {
        log.info("Initializing shuffle executor");
        return new S3AsyncShuffleExecutorComponents(sparkConf, delegate.executor());
    }
}
