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

import java.lang.{Boolean => JBoolean, Integer => JInteger, Long => JLong}
import java.time.{Clock, Duration}
import java.util
import java.util.Optional

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.palantir.spark.shuffle.async.s3.metadata.MapOutputId
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage.BlockManagerId

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

trait ShuffleDriverEndpointRef {
  def isShuffleRegistered(shuffleId: Int): Boolean

  def getNextMergeId: Long

  def registerMergedMapOutput(mapOutputIds: java.util.List[MapOutputId], mergeId: Long): Unit

  def registerUnmergedMapOutput(mapOutputId: MapOutputId): Unit

  def getMapOutputMergeId(mapOutputId: MapOutputId): java.util.Optional[JLong]

  def blacklistExecutor(blockManagerId: BlockManagerId): Unit

  def getBlacklistedExecutors: java.util.Set[BlockManagerId]
}

final class JavaShuffleDriverEndpointRef(shuffleDriverEndpointRef: RpcEndpointRef) extends ShuffleDriverEndpointRef {
  override def isShuffleRegistered(shuffleId: Int): Boolean = {
    shuffleDriverEndpointRef.askSync[Boolean](IsShuffleRegistered(shuffleId))
  }

  override def getNextMergeId: Long = {
    shuffleDriverEndpointRef.askSync[Long](GetNextMergeId)
  }

  override def registerMergedMapOutput(mapOutputIds: java.util.List[MapOutputId], mergeId: Long): Unit = {
    shuffleDriverEndpointRef.send(RegisterMergedMapOutput(mapOutputIds.asScala, mergeId))
  }

  override def registerUnmergedMapOutput(mapOutputId: MapOutputId): Unit = {
    shuffleDriverEndpointRef.send(RegisterUnmergedMapOutput(mapOutputId))
  }

  override def getMapOutputMergeId(mapOutputId: MapOutputId): java.util.Optional[JLong] = {
    shuffleDriverEndpointRef.askSync[Option[Long]](GetMapOutputMergeId(mapOutputId))
      .map(long2Long)
      .asJava
  }

  def blacklistExecutor(blockManagerId: BlockManagerId): Unit = {
    shuffleDriverEndpointRef.send(BlacklistExecutor(blockManagerId))
  }

  def getBlacklistedExecutors: java.util.Set[BlockManagerId] = {
    shuffleDriverEndpointRef.askSync[java.util.Set[BlockManagerId]](GetBlacklistedExecutors)
  }
}

final class CachingShuffleDriverEndpointRef(
    delegate: ShuffleDriverEndpointRef,
    cacheExpirationMillis: Long,
    maxCacheSize: Int,
    clock: Clock) extends ShuffleDriverEndpointRef {
  private val registeredShufflesCache = Caffeine.newBuilder()
    .expireAfterWrite(Duration.ofMillis(cacheExpirationMillis))
    .maximumSize(maxCacheSize)
    .build[JInteger, JBoolean](new CacheLoader[JInteger, JBoolean] {
    override def load(shuffleId: JInteger): JBoolean = delegate.isShuffleRegistered(shuffleId)
  })

  private val mergeIdCache = Caffeine.newBuilder()
    .expireAfterWrite(Duration.ofMillis(cacheExpirationMillis))
    .maximumSize(maxCacheSize)
    .build[MapOutputId, Optional[JLong]](new CacheLoader[MapOutputId, Optional[JLong]] {
    override def load(mapOutputId: MapOutputId): Optional[JLong] = delegate.getMapOutputMergeId(mapOutputId)
  })

  private var timestampLastUpdated = clock.millis()

  override def isShuffleRegistered(shuffleId: Int): Boolean = registeredShufflesCache.get(shuffleId)

  override def getNextMergeId: Long = delegate.getNextMergeId

  override def registerMergedMapOutput(mapOutputIds: util.List[MapOutputId], mergeId: Long): Unit =
    delegate.registerMergedMapOutput(mapOutputIds, mergeId)

  override def registerUnmergedMapOutput(mapOutputId: MapOutputId): Unit = {
    delegate.registerUnmergedMapOutput(mapOutputId)
  }

  override def getMapOutputMergeId(mapOutputId: MapOutputId): Optional[JLong] = mergeIdCache.get(mapOutputId)

  override def blacklistExecutor(blockManagerId: BlockManagerId): Unit = {
    delegate.blacklistExecutor(blockManagerId)
  }

  // Don't cache for blacklisted executors - this is time-critical information that we must always have the latest
  // information for. It'll cost us a large number of RPC calls (equal to the number of reducers) but it's well
  // worth it.
  def getBlacklistedExecutors: java.util.Set[BlockManagerId] = {
    delegate.getBlacklistedExecutors
  }
}
