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

import scala.collection.JavaConverters._

import io.delta.alpine.Snapshot
import io.delta.alpine.internal.exception.DeltaErrors
import io.delta.alpine.internal.util.GoldenTableUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

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
class DeltaLogSuite extends FunSuite {
  // scalastyle:on funsuite
  test("checkpoint") {
    withLogForGoldenTable("checkpoint") { (log, tablePath) =>
      assert(log.snapshot.getVersion == 14)
      assert(log.snapshot.getAllFiles.size == 1)
      assert(log.snapshot.getNumOfFiles == 1)
    }
  }

  test("snapshot") {
    def getDirDataFiles(tablePath: String): Array[File] = {
      val dir = new File(tablePath)
      dir.listFiles().filter(_.isFile).filter(_.getName.endsWith("snappy.parquet"))
    }

    def verifySnapshot(
        snapshot: Snapshot,
        expectedFiles: Array[File],
        expectedVersion: Int): Unit = {
      assert(snapshot.getVersion == expectedVersion)
      assert(snapshot.getAllFiles.size() == expectedFiles.length)
      assert(
        snapshot.getAllFiles.stream().allMatch(f => expectedFiles.exists(_.getName == f.getPath)))
    }

    // Append data0
    var data0_files: Array[File] = Array.empty
    withLogForGoldenTable("snapshot-data0") { (log, tablePath) =>
      data0_files = getDirDataFiles(tablePath) // data0 files
      verifySnapshot(log.snapshot(), data0_files, 0)
    }

    // Append data1
    var data0_data1_files: Array[File] = Array.empty
    withLogForGoldenTable("snapshot-data1") { (log, tablePath) =>
      data0_data1_files = getDirDataFiles(tablePath) // data0 & data1 files
      verifySnapshot(log.snapshot(), data0_data1_files, 1)
    }

    // Overwrite with data2
    var data2_files: Array[File] = Array.empty
    withLogForGoldenTable("snapshot-data2") { (log, tablePath) =>
      // we have overwritten files for data0 & data1; only data2 files should remain
      data2_files = getDirDataFiles(tablePath)
        .filterNot(f => data0_data1_files.exists(_.getName == f.getName))
      verifySnapshot(log.snapshot(), data2_files, 2)
    }

    // Append data3
    withLogForGoldenTable("snapshot-data3") { (log, tablePath) =>
      // we have overwritten files for data0 & data1; only data2 & data3 files should remain
      val data2_data3_files = getDirDataFiles(tablePath)
        .filterNot(f => data0_data1_files.exists(_.getName == f.getName))
      verifySnapshot(log.snapshot(), data2_data3_files, 3)
    }

    // Delete data2 files
    withLogForGoldenTable("snapshot-data2-deleted") { (log, tablePath) =>
      // we have overwritten files for data0 & data1, and deleted data2 files; only data3 files
      // should remain
      val data3_files = getDirDataFiles(tablePath)
        .filterNot(f => data0_data1_files.exists(_.getName == f.getName))
        .filterNot(f => data2_files.exists(_.getName == f.getName))
      verifySnapshot(log.snapshot(), data3_files, 4)
    }

    // Repartition into 2 files
    withLogForGoldenTable("snapshot-repartitioned") { (log, tablePath) =>
      assert(log.snapshot().getNumOfFiles == 2)
      assert(log.snapshot().getVersion == 5)
    }

    // Vacuum
    withLogForGoldenTable("snapshot-vacuumed") { (log, tablePath) =>
      // all remaining dir data files should be needed for current snapshot version
      // vacuum doesn't change the snapshot version
      verifySnapshot(log.snapshot(), getDirDataFiles(tablePath), 5)
    }
  }

  // TODO: this deletes! need to copy into temp dir?
//  test("SC-8078: update deleted directory") {
//    withLogForGoldenTable("update-deleted-directory") { (log, tablePath) =>
//      val path = new Path(tablePath)
//      val fs = path.getFileSystem(new Configuration())
//      fs.delete(path, true)
//
//      assert(log.update().getVersion == -1)
//    }
//  }

  test("update shouldn't pick up delta files earlier than checkpoint") {
    // TODO not sure how
  }

  test("handle corrupted '_last_checkpoint' file") {
    withLogImplForGoldenTable("corrupted-last-checkpoint") { (log1, tablePath) =>
      assert(log1.lastCheckpoint.isDefined)

      val lastCheckpoint = log1.lastCheckpoint.get

      // Create an empty "_last_checkpoint" (corrupted)
      val fs = log1.LAST_CHECKPOINT.getFileSystem(log1.hadoopConf)
      fs.create(log1.LAST_CHECKPOINT, true /* overwrite */).close()

      // Create a new DeltaLog
      val log2 = DeltaLogImpl.forTable(new Configuration(), new Path(tablePath))

      // Make sure we create a new DeltaLog in order to test the loading logic.
      assert(log1 ne log2)

      // We should get the same metadata even if "_last_checkpoint" is corrupted.
      assert(CheckpointInstance(log2.lastCheckpoint.get) === CheckpointInstance(lastCheckpoint))
    }
  }

  test("paths should be canonicalized - normal characters") {
    withLogForGoldenTable("canonicalized-paths-normal-a") { (log, tablePath) =>
      assert(log.update().getVersion == 1)
      assert(log.snapshot.getNumOfFiles == 0)
    }

    withLogForGoldenTable("canonicalized-paths-normal-b") { (log, tablePath) =>
      assert(log.update().getVersion == 1)
      assert(log.snapshot.getNumOfFiles == 0)
    }
  }

  test("paths should be canonicalized - special characters") {
    withLogForGoldenTable("canonicalized-paths-special-a") { (log, tablePath) =>
      assert(log.update().getVersion == 1)
      assert(log.snapshot.getNumOfFiles == 0)
    }

    withLogForGoldenTable("canonicalized-paths-special-b") { (log, tablePath) =>
      assert(log.update().getVersion == 1)
      assert(log.snapshot.getNumOfFiles == 0)
    }
  }

  test("delete and re-add the same file in different transactions") {
    withLogForGoldenTable("delete-re-add-same-file-different-transactions") { (log, tablePath) =>
      assert(log.snapshot().getAllFiles.size() == 2)

      assert(log.snapshot().getAllFiles.asScala.map(_.getPath).toSet == Set("foo", "bar"))

      // We added two add files with the same path `foo`. The first should have been removed.
      // The second should remain, and should have a hard-coded modification time of 1700000000000L
      assert(log.snapshot().getAllFiles.asScala.find(_.getPath == "foo").get.getModificationTime
        == 1700000000000L)
    }
  }

  test("error - versions not contiguous") {
      val ex = intercept[IllegalStateException] {
        withLogForGoldenTable("versions-not-contiguous") { (log, tablePath) => }
      }

      assert(ex.getMessage ===
        DeltaErrors.deltaVersionsNotContiguousException(Vector(0, 2)).getMessage)
  }
}
