/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.standalone.internal

import java.util.{Calendar, TimeZone}

import io.delta.standalone.internal.util.FileNames.{checkpointPrefix, isCheckpointFile, isDeltaFile, checkpointVersion, deltaVersion}

import org.apache.commons.lang.time.DateUtils
import org.apache.hadoop.fs.{FileStatus, Path}

trait MetadataCleanup {
  self: DeltaLogImpl =>

  /**
   * Returns the duration in millis for how long to keep around obsolete logs. We may keep logs
   * beyond this duration until the next calendar day to avoid constantly creating checkpoints.
   */
  def deltaRetentionMillis: Long = {
    // TODO: DeltaConfigs.LOG_RETENTION
    // 30 days
    1000 * 60 * 60 * 24 * 30
  }

  /** Clean up expired delta and checkpoint logs. Exposed for testing. */
  def doLogCleanup(): Unit = {
    val fileCutOffTime = truncateDay(clock.getTimeMillis() - deltaRetentionMillis).getTime
    listExpiredDeltaLogs(fileCutOffTime.getTime).map(_.getPath).foreach { path =>
      // recursive = false
      fs.delete(path, false)
    }
  }

  /**
   * Returns an iterator of expired delta logs that can be cleaned up. For a delta log to be
   * considered as expired, it must:
   *  - have a checkpoint file after it
   *  - be older than `fileCutOffTime`
   */
  private def listExpiredDeltaLogs(fileCutOffTime: Long): Iterator[FileStatus] = {
    val latestCheckpoint = lastCheckpoint
    if (latestCheckpoint.isEmpty) return Iterator.empty
    val threshold = latestCheckpoint.get.version - 1L
    val files = store.listFrom(checkpointPrefix(logPath, 0))
      .filter(f => isCheckpointFile(f.getPath) || isDeltaFile(f.getPath))
    def getVersion(filePath: Path): Long = {
      if (isCheckpointFile(filePath)) {
        checkpointVersion(filePath)
      } else {
        deltaVersion(filePath)
      }
    }

    new BufferingLogDeletionIterator(files, fileCutOffTime, threshold, getVersion)
  }

  /** Truncates a timestamp down to the previous midnight and returns the time and a log string */
  private def truncateDay(timeMillis: Long): Calendar = {
    val date = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    date.setTimeInMillis(timeMillis)

    // TODO: this is using org.apache.commons.lang instead of org.apache.commons.lang3
    DateUtils.truncate(
      date,
      Calendar.DAY_OF_MONTH)
  }
}
