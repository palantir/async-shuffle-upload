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

package com.palantir.spark.shuffle.async.s3.reader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.palantir.shuffle.async.ShuffleDriverEndpointRef;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.FetchFailedException;
import org.apache.spark.shuffle.api.ShuffleBlockInfo;
import org.apache.spark.shuffle.api.ShuffleBlockInputStream;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.ShuffleBlockAttemptId;
import org.apache.spark.storage.ShuffleBlockId;
import org.apache.spark.storage.ShuffleDataBlockId;
import org.apache.spark.storage.ShuffleIndexBlockId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FallbackToS3ShuffleIterator implements Iterator<ShuffleBlockInputStream> {

    private static final Logger LOG = LoggerFactory.getLogger(FallbackToS3ShuffleIterator.class);

    private final Iterator<ShuffleBlockInputStream> fetchFromExecutorsIterator;
    private final S3FetcherIterator s3FetcherIterator;
    private final Map<ShuffleBlockId, ShuffleBlockInfo> remainingAttemptsByBlock;
    private final boolean shouldCompressShuffle;
    private final SerializerManager serializerManager;
    private final CompressionCodec compressionCodec;
    private final ShuffleDriverEndpointRef driverEndpointRef;

    public FallbackToS3ShuffleIterator(
            Iterator<ShuffleBlockInputStream> fetchFromExecutorsIterator,
            Set<ShuffleBlockInfo> shuffleBlocksFromExecutor,
            boolean shouldCompressShuffle,
            SerializerManager serializerManager,
            CompressionCodec compressionCodec,
            Set<ShuffleBlockInfo> shuffleBlocksFromRemote,
            S3FetcherIterator s3FetcherIterator,
            ShuffleDriverEndpointRef driverEndpointRef) {
        this.fetchFromExecutorsIterator = fetchFromExecutorsIterator;
        this.shouldCompressShuffle = shouldCompressShuffle;
        this.serializerManager = serializerManager;
        this.compressionCodec = compressionCodec;
        this.remainingAttemptsByBlock =
                Maps.newHashMapWithExpectedSize(shuffleBlocksFromExecutor.size() + shuffleBlocksFromRemote.size());
        shuffleBlocksFromExecutor.forEach(block -> remainingAttemptsByBlock.put(
                new ShuffleBlockId(block.getShuffleId(), block.getMapId(), block.getReduceId()),
                block));
        shuffleBlocksFromRemote.forEach(block -> remainingAttemptsByBlock.put(
                new ShuffleBlockId(block.getShuffleId(), block.getMapId(), block.getReduceId()),
                block));
        this.s3FetcherIterator = s3FetcherIterator;
        Preconditions.checkState(!s3FetcherIterator.isInitialized(),
                "S3 Fetcher iterator already initialized");
        this.driverEndpointRef = driverEndpointRef;
    }

    @Override
    public boolean hasNext() {
        return !remainingAttemptsByBlock.isEmpty();
    }

    @Override
    public ShuffleBlockInputStream next() {
        ShuffleBlockInputStream resultStream = null;
        while (resultStream == null && hasNext()) {
            if (!s3FetcherIterator.isInitialized() && fetchFromExecutorsIterator.hasNext()) {
                try {
                    resultStream = fetchFromExecutorsIterator.next();
                    BlockId resultBlock = resultStream.getBlockId();
                    ShuffleBlockId resolvedBlockId = convertBlockId(resultBlock);
                    remainingAttemptsByBlock.remove(resolvedBlockId);
                } catch (Throwable e) {
                    if (e instanceof FetchFailedException) {
                        LOG.warn(
                                "Failed to fetch block the regular way, due to a fetch failed"
                                        + " exception. Fetching from the hadoop file system instead.",
                                e);
                        ShuffleBlockInfo blockInfo =
                                remainingAttemptsByBlock.get(((FetchFailedException) e).getShuffleBlockId());
                        driverEndpointRef.blacklistExecutor(blockInfo.getShuffleLocation().get());
                    }
                    throw e;
                }
            } else if (!s3FetcherIterator.isInitialized()) {
                // When we get a fetch failed exception, switch to only using the S3 fetcher iterator.
                // Find all blocks that were not yet successfully fetched from the executors.
                // Request all remaining blocks from remote storage.
                // Not necessarily 100% accurate - for example, there may be other executors
                // remaining where we could have fetched the data from. But:

                // 1. The lifecycle of using the underlying iterator that fetches from other
                //    executors, is more or less unpredictable after a fetch failure. And,
                // 2. It's better to go to remote storage excessively, than attempting to fetch blocks
                //    from any executors that might be down at all - since attempting to fetch from
                //    dead executors requires waiting for a costly timeout.
                s3FetcherIterator.initialize(ImmutableList.copyOf(remainingAttemptsByBlock.values()));
                resultStream = fetchFromS3();
            } else {
                resultStream = fetchFromS3();
            }
        }
        // TODO(mcheah): #8 this should be FetchFailedException, not IllegalStateException.
        if (resultStream == null) {
            throw new SafeIllegalStateException("Could not fetch shuffle blocks from either the distributed"
                    + " store or the mapper executor.");
        }
        return resultStream;
    }

    private ShuffleBlockInputStream fetchFromS3() {
        Preconditions.checkNotNull(s3FetcherIterator, "S3 fetcher iterator expected to not be null");
        ShuffleBlockInputStream resultStream = s3FetcherIterator.next();
        BlockId blockId = resultStream.getBlockId();
        InputStream resultDeserialized = resultStream;
        if (shouldCompressShuffle) {
            resultDeserialized = compressionCodec.compressedInputStream(
                    serializerManager.wrapForEncryption(resultDeserialized));
        } else {
            resultDeserialized = serializerManager.wrapForEncryption(resultDeserialized);
        }
        remainingAttemptsByBlock.remove(convertBlockId(blockId));

        if (resultDeserialized == resultStream) {
            return resultStream;
        } else {
            return new ShuffleBlockInputStream(blockId, resultDeserialized);
        }
    }

    public void cleanup() {
        s3FetcherIterator.cleanup();
    }

    private static ShuffleBlockId convertBlockId(BlockId blockId) {
        int shuffleId;
        int mapId;
        int reduceId;
        if (blockId instanceof ShuffleBlockId) {
            return (ShuffleBlockId) blockId;
        } else if (blockId instanceof ShuffleBlockAttemptId) {
            shuffleId = ((ShuffleBlockAttemptId) blockId).shuffleId();
            mapId = ((ShuffleBlockAttemptId) blockId).mapId();
            reduceId = ((ShuffleBlockAttemptId) blockId).reduceId();
        } else if (blockId instanceof ShuffleDataBlockId) {
            shuffleId = ((ShuffleDataBlockId) blockId).shuffleId();
            mapId = ((ShuffleDataBlockId) blockId).mapId();
            reduceId = ((ShuffleDataBlockId) blockId).reduceId();
        } else if (blockId instanceof ShuffleIndexBlockId) {
            shuffleId = ((ShuffleIndexBlockId) blockId).shuffleId();
            mapId = ((ShuffleIndexBlockId) blockId).mapId();
            reduceId = ((ShuffleIndexBlockId) blockId).reduceId();
        } else {
            throw new SafeIllegalArgumentException(
                    "Block id is not valid - must be a shuffle block id.",
                    SafeArg.of("blockId", blockId));
        }
        return new ShuffleBlockId(shuffleId, mapId, reduceId);
    }
}
