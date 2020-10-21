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

// scalastyle:off funsuite
import java.io.File

import io.delta.alpine.{DeltaLog, Snapshot}
import io.delta.alpine.internal.exception.DeltaErrors
import io.delta.alpine.internal.util.GoldenTableUtils._
import org.scalatest.FunSuite

import org.apache.spark.sql.delta.util.FileNames

class DeltaTimeTravelSuite extends FunSuite {
  // scalastyle:on funsuite

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

  test("versionAsOf") {
    var correct_version0_data_files: Array[File] = Array.empty
    var correct_version1_data_files: Array[File] = Array.empty
    var correct_version2_data_files: Array[File] = Array.empty

    withGoldenTable("time-travel-versionAsOf-version-0") { tablePath =>
      correct_version0_data_files = getDirDataFiles(tablePath)
    }

    withGoldenTable("time-travel-versionAsOf-version-1") { tablePath =>
      correct_version1_data_files = getDirDataFiles(tablePath)
    }

    withGoldenTable("time-travel-versionAsOf-version-2") { tablePath =>
      correct_version2_data_files = getDirDataFiles(tablePath)
    }

    withLogForGoldenTable("time-travel-versionAsOf-version-2") { (log, tablePath) =>
      // Correct cases
      verifySnapshot(log.getSnapshotForVersionAsOf(0), correct_version0_data_files, 0)
      verifySnapshot(log.getSnapshotForVersionAsOf(1), correct_version1_data_files, 1)
      verifySnapshot(log.getSnapshotForVersionAsOf(2), correct_version2_data_files, 2)

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
        log.getSnapshotForVersionAsOf(-1)
      }
      assert(e3.getMessage == DeltaErrors.noReproducibleHistoryFound(log.getLogPath).getMessage)
    }
  }
}
