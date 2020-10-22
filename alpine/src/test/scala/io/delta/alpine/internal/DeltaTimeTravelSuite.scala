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
import java.nio.file.Files
import java.sql.Timestamp
import java.util.UUID

import scala.concurrent.duration._
import scala.language.implicitConversions

import io.delta.alpine.{DeltaLog, Snapshot}
import io.delta.alpine.internal.exception.DeltaErrors
import io.delta.alpine.internal.util.FileNames
import io.delta.alpine.internal.util.GoldenTableUtils._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration

// scalastyle:off funsuite
import org.scalatest.FunSuite

/**
 * Instead of using Spark in this project to WRITE data and log files for tests, we have
 * io.delta.golden.GoldenTables do it instead. During tests, we then refer by name to specific
 * golden tables that that class is responsible for generating ahead of time. This allows us to
 * focus on READING only so that we may fully decouple from Spark and not have it as a dependency.
 *
 * See io.delta.golden.GoldenTables for documentation on how to ensure that the needed files have
 * been generated.
 */
class DeltaTimeTravelSuite extends FunSuite {
  // scalastyle:on funsuite

  /** Same start time as used in GoldenTables */
  private val start = 1540415658000L

  private implicit def durationToLong(duration: FiniteDuration): Long = {
    duration.toMillis
  }

  private def getDirDataFiles(tablePath: String): Array[File] = {
    val dir = new File(tablePath)
    dir.listFiles().filter(_.isFile).filter(_.getName.endsWith("snappy.parquet"))
  }

  private def verifySnapshot(
      snapshot: Snapshot,
      expectedFiles: Array[File],
      expectedVersion: Int): Unit = {
    assert(snapshot.getVersion == expectedVersion)
    assert(snapshot.getAllFiles.size() == expectedFiles.length)
    assert(
      snapshot.getAllFiles.stream().allMatch(f => expectedFiles.exists(_.getName == f.getPath)))
  }

  var start_data_files: Array[File] = Array.empty
  var start_start20_data_files: Array[File] = Array.empty
  var start_start20_start40_data_files: Array[File] = Array.empty

  withGoldenTable("time-travel-start") { tablePath =>
    start_data_files = getDirDataFiles(tablePath)
  }

  withGoldenTable("time-travel-start-start20") { tablePath =>
    start_start20_data_files = getDirDataFiles(tablePath)
  }

  withGoldenTable("time-travel-start-start20-start40") { tablePath =>
    start_start20_start40_data_files = getDirDataFiles(tablePath)
  }

  /**
   * `Error case - not reproducible` needs to delete the log directory. Since we don't want to
   * delete the golden tables, we instead copy the table into a temp directory, deleting that temp
   * directory when we are done.
   */
  test("versionAsOf") {
    withGoldenTable("time-travel-start-start20-start40") { tablePath =>
      val tempDir = Files.createTempDirectory(UUID.randomUUID().toString).toFile
      try {
        FileUtils.copyDirectory(new File(tablePath), tempDir)
        val log = DeltaLog.forTable(new Configuration(), tempDir)

        // Correct cases
        verifySnapshot(log.getSnapshotForVersionAsOf(0), start_data_files, 0)
        verifySnapshot(log.getSnapshotForVersionAsOf(1), start_start20_data_files, 1)
        verifySnapshot(log.getSnapshotForVersionAsOf(2), start_start20_start40_data_files, 2)

        // Error case - version after latest commit
        val e1 = intercept[DeltaErrors.DeltaTimeTravelException] {
          log.getSnapshotForVersionAsOf(3)
        }
        assert(e1.getMessage == DeltaErrors.versionNotExistException(3, 0, 2).getMessage)

        // Error case - version before earliest commit
        val e2 = intercept[DeltaErrors.DeltaTimeTravelException] {
          log.getSnapshotForVersionAsOf(-1)
        }
        assert(e2.getMessage == DeltaErrors.versionNotExistException(-1, 0, 2).getMessage)

        // Error case - not reproducible
        new File(FileNames.deltaFile(log.getLogPath, 0).toUri).delete()
        val e3 = intercept[DeltaErrors.DeltaTimeTravelException] {
          log.getSnapshotForVersionAsOf(0)
        }
        assert(e3.getMessage == DeltaErrors.noReproducibleHistoryFound(log.getLogPath).getMessage)
      } finally {
        FileUtils.deleteDirectory(tempDir)
      }
    }
  }

  test("timestampAsOf with timestamp in between commits - should use commit before timestamp") {
    withLogForGoldenTable("time-travel-start-start20-start40") { (log, _) =>
      verifySnapshot(
        log.getSnapshotForTimestampAsOf(start + 10.minutes), start_data_files, 0)
      verifySnapshot(
        log.getSnapshotForTimestampAsOf(start + 30.minutes), start_start20_data_files, 1)
    }
  }

  test("timestampAsOf with timestamp after last commit should fail") {
    withLogForGoldenTable("time-travel-start-start20-start40") { (log, _) =>
      val e = intercept[DeltaErrors.DeltaTimeTravelException] {
        log.getSnapshotForTimestampAsOf(start + 50.minutes) // later by 10 mins
      }

      val latestTimestamp = new Timestamp(start + 40.minutes)
      val usrTimestamp = new Timestamp(start + 50.minutes)
      assert(e.getMessage ==
        DeltaErrors.timestampLaterThanTableLastCommit(usrTimestamp, latestTimestamp).getMessage)
    }
  }

  test("timestampAsOf with timestamp on exact commit timestamp") {
    withLogForGoldenTable("time-travel-start-start20-start40") { (log, _) =>
      verifySnapshot(
        log.getSnapshotForTimestampAsOf(start), start_data_files, 0)
      verifySnapshot(
        log.getSnapshotForTimestampAsOf(start + 20.minutes), start_start20_data_files, 1)
      verifySnapshot(
        log.getSnapshotForTimestampAsOf(start + 40.minutes), start_start20_start40_data_files, 2)
    }
  }

  test("time travel with schema changes - should instantiate old schema") {
    var orig_schema_data_files: Array[File] = Array.empty
    // write data to a table with some original schema
    withGoldenTable("time-travel-schema-changes-a") { tablePath =>
      orig_schema_data_files = getDirDataFiles(tablePath)
    }

    // then append more data to that "same" table using a different schema
    // reading version 0 should show only the original-schema data files
    withLogForGoldenTable("time-travel-schema-changes-b") { (log, _) =>
      verifySnapshot(log.getSnapshotForVersionAsOf(0), orig_schema_data_files, 0)
    }
  }
}
