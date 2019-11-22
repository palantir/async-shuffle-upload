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

import com.palantir.spark.shuffle.async.s3.metadata.{BlacklistedExecutorTracker, ShuffleIdTracker}
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}

final class AsyncShuffleUploadDriverEndpoint(
    override val rpcEnv: RpcEnv,
    shuffleIdTracker: ShuffleIdTracker,
    blacklistedExecutorTracker: BlacklistedExecutorTracker) extends ThreadSafeRpcEndpoint {

  override def receiveAndReply(context: RpcCallContext)
      : PartialFunction[Any, Unit] = {
    case IsShuffleRegistered(shuffleId) =>
      context.reply(shuffleIdTracker.isShuffleRegistered(shuffleId))
    case GetNextMergeId => context.reply(shuffleIdTracker.getNextMergeId())
    case GetMapOutputMergeId(mapOutputId) =>
      context.reply(shuffleIdTracker.getMergeId(mapOutputId))
    case GetBlacklistedExecutors => context.reply(blacklistedExecutorTracker.getBlacklistedExecutors)
  }

  override def receive(): PartialFunction[Any, Unit] = {
    case RegisterMergedMapOutput(mapOutputIds, mergeId) =>
      shuffleIdTracker.registerMergedOutput(mapOutputIds, mergeId)
    case RegisterUnmergedMapOutput(mapOutputId) =>
      shuffleIdTracker.registerUnmergedMapOutput(mapOutputId)
    case BlacklistExecutor(blockManagerId) =>
      blacklistedExecutorTracker.blacklistExecutor(blockManagerId)
  }
}

object AsyncShuffleUploadDriverEndpoint {
  val NAME = "async-shuffle-driver-endpoint"
}
