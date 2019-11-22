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

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.ConfigEntry

case class JavaSparkConf(conf: SparkConf) {

  def getInt(key: ConfigEntry[Int]): Int = conf.get(key)

  def getLong(key: ConfigEntry[Long]): Long = conf.get(key)

  def get[T](key: ConfigEntry[T]): T = conf.get(key)
}
