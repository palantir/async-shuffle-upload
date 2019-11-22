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

import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.BlockManagerId

object FetchFailedExceptionThrower {

  /**
    * A workaround for the fact that {@link FetchFailedException} is a checked exception and hence it cannot be thrown
    * by any Java method that does not declare it in the method signature.
    */
  def throwFetchFailedException[T](
      shuffleId: Int,
      mapId: Int,
      reduceId: Int,
      bmId: BlockManagerId,
      errorMessage: String,
      cause: Throwable): T = {
    throw new FetchFailedException(
      bmId, shuffleId, mapId, reduceId, errorMessage, cause)
  }
}
