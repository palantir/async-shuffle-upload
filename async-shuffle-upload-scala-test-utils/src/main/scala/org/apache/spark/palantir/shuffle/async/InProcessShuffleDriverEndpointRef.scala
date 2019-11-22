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

import java.util.concurrent.ExecutorService

import com.google.common.util.concurrent.SettableFuture
import org.apache.spark.SparkConf
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEndpointRef, RpcTimeout}

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

class InProcessShuffleDriverEndpointRef(
     conf: SparkConf,
     shuffleUploadDriverEndpoint: AsyncShuffleUploadDriverEndpoint,
     askExecutor: ExecutorService) extends RpcEndpointRef(conf) {

  private implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutorService(askExecutor)

  override def address: RpcAddress = RpcAddress("localhost", 0)

  override def name: String = "test-in-process-driver-endpoint"

  override def send(message: Any): Unit = shuffleUploadDriverEndpoint.receive()(message)

  override def ask[T](message: Any, timeout: RpcTimeout)(implicit evidence$1: ClassTag[T]): Future[T] = {
    Future[T] {
      val result = SettableFuture.create[T]()
      shuffleUploadDriverEndpoint.receiveAndReply(new RpcCallContext {
        override def reply(response: Any): Unit = result.set(response.asInstanceOf[T])

        override def sendFailure(e: Throwable): Unit = result.setException(e)

        override def senderAddress: RpcAddress = address
      })(message)
      result.get()
    }
  }
}
