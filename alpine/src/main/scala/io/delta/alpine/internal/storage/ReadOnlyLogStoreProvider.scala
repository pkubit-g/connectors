/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.alpine.internal.storage

import io.delta.alpine.ReadOnlyLogStore
import io.delta.alpine.internal.util.TypeUtils
import io.delta.alpine.sources.AlpineHadoopConf
import org.apache.hadoop.conf.Configuration

private[internal] trait ReadOnlyLogStoreProvider {
  lazy val defaultLogStoreClass: String = classOf[HDFSReadOnlyLogStore].getName

  def createLogStore(hadoopConf: Configuration): ReadOnlyLogStore = {
    val logStoreClassName =
      hadoopConf.get(AlpineHadoopConf.LOG_STORE_CLASS_KEY, defaultLogStoreClass)
    val logStoreClass = TypeUtils.classForName(logStoreClassName)

    logStoreClass.getConstructor(classOf[Configuration]).newInstance(hadoopConf)
      .asInstanceOf[ReadOnlyLogStore]
  }
}
