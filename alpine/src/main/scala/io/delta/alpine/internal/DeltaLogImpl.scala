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


package io.delta.alpine.internal

import java.io.File
import java.util.concurrent.locks.ReentrantLock

import io.delta.alpine.DeltaLog
import io.delta.alpine.internal.storage.ReadOnlyLogStoreProvider
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

private[internal] class DeltaLogImpl private(
    val hadoopConf: Configuration,
    val logPath: Path,
    val dataPath: Path)
  extends DeltaLog
  with Checkpoints
  with SnapshotManagement
  with ReadOnlyLogStoreProvider {

  override def getHadoopConf: Configuration = hadoopConf

  override def getLogPath: Path = logPath

  override def getDataPath: Path = dataPath

  lazy val store = createLogStore(hadoopConf)

  private val deltaLogLock = new ReentrantLock()

  protected def lockInterruptibly[T](body: => T): T = {
    deltaLogLock.lockInterruptibly()
    try {
      body
    } finally {
      deltaLogLock.unlock()
    }
  }
}

object DeltaLogImpl {
  def forTable(hadoopConf: Configuration, dataPath: String): DeltaLogImpl = {
    apply(hadoopConf, new Path(dataPath, "_delta_log"))
  }

  def forTable(hadoopConf: Configuration, dataPath: File): DeltaLogImpl = {
    apply(hadoopConf, new Path(dataPath.getAbsolutePath, "_delta_log"))
  }

  def forTable(hadoopConf: Configuration, dataPath: Path): DeltaLogImpl = {
    apply(hadoopConf, new Path(dataPath, "_delta_log"))
  }

  def apply(hadoopConf: Configuration, rawPath: Path): DeltaLogImpl = {
    val fs = rawPath.getFileSystem(hadoopConf)
    val path = fs.makeQualified(rawPath)

    new DeltaLogImpl(hadoopConf, path, path.getParent)
  }
}
