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

package com.palantir.spark.shuffle.async.s3.metadata;

import java.lang.{Integer => JInt, Long => JLong}
import java.util
import java.util.concurrent.atomic.AtomicLong

import com.google.common.collect.Maps
import com.palantir.logsafe.SafeArg
import com.palantir.spark.shuffle.async.s3.metadata.ShuffleIdTracker._
import org.slf4j.{Logger, LoggerFactory}

import scala.compat.java8.FunctionConverters._

final class ShuffleIdTracker {

  private val registeredShufflesToBackupMaps = Maps.newConcurrentMap[JInt, MapIdsToAttempts]()
  private val nextMergeId = new AtomicLong()

  def registerShuffle(shuffleId: Int): Unit = {
    registeredShufflesToBackupMaps.putIfAbsent(shuffleId, MapIdsToAttempts())
    log.debug("Registered shuffle id", shuffleIdArg(shuffleId))
  }

  def unregisterShuffle(shuffleId: Int): Unit = {
    registeredShufflesToBackupMaps.remove(shuffleId)
    log.debug("Unregistered shuffle id", shuffleIdArg(shuffleId))
  }

  def isShuffleRegistered(shuffleId: Int): Boolean = {
    registeredShufflesToBackupMaps.containsKey(shuffleId)
  }

  def isMapOutputRegistered(shuffleId: Int, mapId: Int, mapTaskAttemptId: Long): Boolean = {
    val registered = Option(registeredShufflesToBackupMaps.get(shuffleId))
      .flatMap(_(mapId))
      .exists(_.hasAttempt(mapTaskAttemptId))
    log.debug("Checking if map id is registered",
      shuffleIdArg(shuffleId),
      mapIdArg(mapId),
      SafeArg.of("mapTaskAttemptId", mapTaskAttemptId),
      SafeArg.of("registered", registered))
    registered
  }

  def getNextMergeId(): Long = nextMergeId.getAndIncrement()

  def registerMergedOutput(mapOutputs: Iterable[MapOutputId], mergeId: Long): Unit = {
    mapOutputs.foreach(mapOutput => {
      val shuffleId = mapOutput.shuffleId
      registeredShufflesToBackupMaps.computeIfPresent(
        shuffleId,
        asJavaBiFunction((_: JInt, mapIdsToAttempts: MapIdsToAttempts) => {
          mapIdsToAttempts
            .getOrRegisterMapId(mapOutput.mapId)
            .putAttempt(mapOutput.mapAttemptId, Some(mergeId))
          log.debug("Registering mapOutput with merged output",
            SafeArg.of("mergeId", mergeId),
            shuffleIdArg(shuffleId),
            mapIdArg(mapOutput.mapId),
            SafeArg.of("mapAttemptId", mapOutput.mapAttemptId))
          mapIdsToAttempts
      }))
    })
  }

  def registerUnmergedMapOutput(mapOutput: MapOutputId): Unit = {
    registeredShufflesToBackupMaps.computeIfPresent(
      mapOutput.shuffleId,
      asJavaBiFunction((_: JInt, mapIdsToAttempts: MapIdsToAttempts) => {
        mapIdsToAttempts.getOrRegisterMapId(mapOutput.mapId).putAttempt(mapOutput.mapAttemptId)
        mapIdsToAttempts
      })
    )
    log.debug("Registered mapOutput",
      shuffleIdArg(mapOutput.shuffleId),
      mapIdArg(mapOutput.mapId),
      SafeArg.of("mapAttemptId", mapOutput.mapAttemptId))
  }

  def getMergeId(mapOutputId: MapOutputId): Option[Long] = {
    Option(registeredShufflesToBackupMaps.get(mapOutputId.shuffleId))
      .flatMap(_(mapOutputId.mapId))
      .flatMap(_(mapOutputId.mapAttemptId))
      .map(Long2long)
  }

  // Effectively serves as a thin wrapper / typedef around a concurrent hash map.
  private final class BackupAttemptIdsToMergeIds {
    private val attemptsToMergeIds = Maps.newConcurrentMap[JLong, Option[JLong]]()

    def apply(attempt: Long): Option[JLong] = {
      Option(attemptsToMergeIds.get(attempt)).flatten
    }

    def hasAttempt(attempt: Long): Boolean = {
      attemptsToMergeIds.containsKey(attempt)
    }

    def putAttempt(attempt: Long, mergeId: Option[Long] = None): Unit = {
      attemptsToMergeIds.putIfAbsent(attempt, mergeId.map(long2Long))
    }
  }

  private object BackupAttemptIdsToMergeIds {
    def apply(): BackupAttemptIdsToMergeIds = new BackupAttemptIdsToMergeIds()
  }

  // Effectively serves as a thin wrapper / typedef around a concurrent hash map.
  private final class MapIdsToAttempts {
    private val idsToAttempts = Maps.newConcurrentMap[JInt, BackupAttemptIdsToMergeIds]()

    def apply(mapId: Int): Option[BackupAttemptIdsToMergeIds] = {
      Option(idsToAttempts.get(mapId))
    }

    def getOrRegisterMapId(mapId: Int): BackupAttemptIdsToMergeIds = {
      idsToAttempts.computeIfAbsent(
        mapId,
        asJavaFunction((_: JInt) => BackupAttemptIdsToMergeIds()))
    }

    def getRegisteredMaps(): util.Set[JInt] = {
      idsToAttempts.keySet()
    }
  }

  private object MapIdsToAttempts {
    def apply(): MapIdsToAttempts = new MapIdsToAttempts()
  }
}

object ShuffleIdTracker {
  private val log: Logger = LoggerFactory.getLogger(classOf[ShuffleIdTracker])
  private def shuffleIdArg(shuffleId: Int): SafeArg[Int] = SafeArg.of("shuffleId", shuffleId)
  private def mapIdArg(mapId: Int): SafeArg[Int] = SafeArg.of("mapId", mapId)
}
