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

package io.delta.alpine.internal.exception

import java.io.FileNotFoundException

import io.delta.alpine.types.StructType
import org.apache.hadoop.fs.Path

private[internal] object DeltaErrors {

  // TODO make this RuntimeException?
  class DeltaTimeTravelException(message: String) extends Exception(message)

  def deltaVersionsNotContiguousException(deltaVersions: Seq[Long]): Throwable = {
    new IllegalStateException(s"Versions ($deltaVersions) are not contiguous.")
  }

  def emptyDirectoryException(directory: String): Throwable = {
    new FileNotFoundException(s"No file found in the directory: $directory.")
  }

  def logFileNotFoundException(
      path: Path,
      version: Long): Throwable = {
    // TODO: use DeltaConfigs.LOG_RETENTION, CHECKPOINT_RETENTION_DURATION for extra info
    new FileNotFoundException(s"$path: Unable to reconstruct state at version $version as the " +
      s"transaction log has been truncated due to manual deletion or the log retention policy ")
  }

  def missingPartFilesException(version: Long, ae: Exception): Throwable = {
    new IllegalStateException(
      s"Couldn't find all part files of the checkpoint version: $version", ae)
  }

  def noReproducibleHistoryFound(logPath: Path): Throwable = {
    new DeltaTimeTravelException(s"No reproducible commits found at $logPath")
  }

  def timestampEarlierThanTableFirstCommit(
      userTimestamp: java.sql.Timestamp,
      commitTs: java.sql.Timestamp): Throwable = {
    new DeltaTimeTravelException(
      s"""The provided timestamp ($userTimestamp) is before the earliest version available to this
         |table ($commitTs). Please use a timestamp greater than or equal to $commitTs.
       """.stripMargin)
  }

  def timestampLaterThanTableLastCommit(
      userTimestamp: java.sql.Timestamp,
      commitTs: java.sql.Timestamp): Throwable = {
    new DeltaTimeTravelException(
      s"""The provided timestamp ($userTimestamp) is after the latest version available to this
         |table ($commitTs). Please use a timestamp less than or equal to $commitTs.
       """.stripMargin)
  }

  def noHistoryFound(logPath: Path): Throwable = {
    new DeltaTimeTravelException(s"No commits found at $logPath")
  }

  def versionNotExistException(userVersion: Long, earliest: Long, latest: Long): Throwable = {
    new DeltaTimeTravelException(s"Cannot time travel Delta table to version $userVersion. " +
      s"Available versions: [$earliest, $latest].")
  }

  def noFieldFoundInSchema(fieldName: String, schema: StructType): Throwable = {
    new IllegalArgumentException(s"No matching schema column for field $fieldName. " +
      s"Schema: ${schema.getTreeString}")
  }

  def nullValueFoundForNonNullSchemaField(fieldName: String, schema: StructType): Throwable = {
    new RuntimeException(s"Read a null value for field $fieldName, yet schema indicates " +
      s"that this field can't be null. Schema: ${schema.getTreeString}")
  }
}
