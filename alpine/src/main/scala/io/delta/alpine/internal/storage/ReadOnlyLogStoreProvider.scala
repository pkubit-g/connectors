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
import io.delta.alpine.sources.AlpineHadoopConf
import org.apache.hadoop.conf.Configuration

private[internal] trait ReadOnlyLogStoreProvider {
  import ReadOnlyLogStoreProvider._

  def createLogStore(hadoopConf: Configuration): ReadOnlyLogStore = {
    val logStoreClassName =
      hadoopConf.get(AlpineHadoopConf.LOG_STORE_CLASS_KEY, defaultLogStoreClassName)

    // scalastyle:off classforname
    val logStoreClass =
      Class.forName(logStoreClassName, true, Thread.currentThread().getContextClassLoader)
    // scalastyle:on classforname

    logStoreClass.getConstructor(classOf[Configuration]).newInstance(hadoopConf)
      .asInstanceOf[ReadOnlyLogStore]
  }
}

private[internal] object ReadOnlyLogStoreProvider {
  val defaultLogStoreClassName: String = classOf[HDFSReadOnlyLogStore].getName
}
