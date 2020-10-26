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
import java.util.UUID

import scala.collection.JavaConverters._

import io.delta.alpine.{DeltaLog, Snapshot}
import io.delta.alpine.internal.exception.DeltaErrors
import io.delta.alpine.internal.util.GoldenTableUtils._
import org.apache.commons.io.FileUtils
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
    withLogForGoldenTable("checkpoint") { log =>
      assert(log.snapshot.getVersion == 14)
      assert(log.snapshot.getAllFiles.size == 1)
      assert(log.snapshot.getNumOfFiles == 1)
    }
  }

  test("snapshot") {
    def getDirDataFiles(tablePath: String): Array[File] = {
      val correctTablePath =
        if (tablePath.startsWith("file:")) tablePath.stripPrefix("file:") else tablePath
      val dir = new File(correctTablePath)
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
    withLogForGoldenTable("snapshot-data0") { log =>
      data0_files = getDirDataFiles(log.getDataPath.toString) // data0 files
      verifySnapshot(log.snapshot(), data0_files, 0)
    }

    // Append data1
    var data0_data1_files: Array[File] = Array.empty
    withLogForGoldenTable("snapshot-data1") { log =>
      data0_data1_files = getDirDataFiles(log.getDataPath.toString) // data0 & data1 files
      verifySnapshot(log.snapshot(), data0_data1_files, 1)
    }

    // Overwrite with data2
    var data2_files: Array[File] = Array.empty
    withLogForGoldenTable("snapshot-data2") { log =>
      // we have overwritten files for data0 & data1; only data2 files should remain
      data2_files = getDirDataFiles(log.getDataPath.toString)
        .filterNot(f => data0_data1_files.exists(_.getName == f.getName))
      verifySnapshot(log.snapshot(), data2_files, 2)
    }

    // Append data3
    withLogForGoldenTable("snapshot-data3") { log =>
      // we have overwritten files for data0 & data1; only data2 & data3 files should remain
      val data2_data3_files = getDirDataFiles(log.getDataPath.toString)
        .filterNot(f => data0_data1_files.exists(_.getName == f.getName))
      verifySnapshot(log.snapshot(), data2_data3_files, 3)
    }

    // Delete data2 files
    withLogForGoldenTable("snapshot-data2-deleted") { log =>
      // we have overwritten files for data0 & data1, and deleted data2 files; only data3 files
      // should remain
      val data3_files = getDirDataFiles(log.getDataPath.toString)
        .filterNot(f => data0_data1_files.exists(_.getName == f.getName))
        .filterNot(f => data2_files.exists(_.getName == f.getName))
      verifySnapshot(log.snapshot(), data3_files, 4)
    }

    // Repartition into 2 files
    withLogForGoldenTable("snapshot-repartitioned") { log =>
      assert(log.snapshot().getNumOfFiles == 2)
      assert(log.snapshot().getVersion == 5)
    }

    // Vacuum
    withLogForGoldenTable("snapshot-vacuumed") { log =>
      // all remaining dir data files should be needed for current snapshot version
      // vacuum doesn't change the snapshot version
      verifySnapshot(log.snapshot(), getDirDataFiles(log.getDataPath.toString), 5)
    }
  }

  test("SC-8078: update deleted directory") {
    withGoldenTable("update-deleted-directory") { tablePath =>
      val tempDir = Files.createTempDirectory(UUID.randomUUID().toString).toFile
      try {
        FileUtils.copyDirectory(new File(tablePath), tempDir)
        val log = DeltaLog.forTable(new Configuration(), tempDir)
        FileUtils.deleteDirectory(tempDir)
        assert(log.update().getVersion == -1)
      } finally {
        // just in case
        FileUtils.deleteDirectory(tempDir)
      }
    }
  }

  test("handle corrupted '_last_checkpoint' file") {
    withLogImplForGoldenTable("corrupted-last-checkpoint") { log1 =>
      assert(log1.lastCheckpoint.isDefined)

      val lastCheckpoint = log1.lastCheckpoint.get

      // Create an empty "_last_checkpoint" (corrupted)
      val fs = log1.LAST_CHECKPOINT.getFileSystem(log1.hadoopConf)
      fs.create(log1.LAST_CHECKPOINT, true /* overwrite */).close()

      // Create a new DeltaLog
      val log2 = DeltaLogImpl.forTable(new Configuration(), new Path(log1.getDataPath.toString))

      // Make sure we create a new DeltaLog in order to test the loading logic.
      assert(log1 ne log2)

      // We should get the same metadata even if "_last_checkpoint" is corrupted.
      assert(CheckpointInstance(log2.lastCheckpoint.get) === CheckpointInstance(lastCheckpoint))
    }
  }

  test("paths should be canonicalized - normal characters") {
    withLogForGoldenTable("canonicalized-paths-normal-a") { log =>
      assert(log.update().getVersion == 1)
      assert(log.snapshot.getNumOfFiles == 0)
    }

    withLogForGoldenTable("canonicalized-paths-normal-b") { log =>
      assert(log.update().getVersion == 1)
      assert(log.snapshot.getNumOfFiles == 0)
    }
  }

  test("paths should be canonicalized - special characters") {
    withLogForGoldenTable("canonicalized-paths-special-a") { log =>
      assert(log.update().getVersion == 1)
      assert(log.snapshot.getNumOfFiles == 0)
    }

    withLogForGoldenTable("canonicalized-paths-special-b") { log =>
      assert(log.update().getVersion == 1)
      assert(log.snapshot.getNumOfFiles == 0)
    }
  }

  test("delete and re-add the same file in different transactions") {
    withLogForGoldenTable("delete-re-add-same-file-different-transactions") { log =>
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
      withLogForGoldenTable("versions-not-contiguous") { _ => }
    }

    assert(ex.getMessage ===
      DeltaErrors.deltaVersionsNotContiguousException(Vector(0, 2)).getMessage)
  }

  Seq("protocol", "metadata").foreach { action =>
    test(s"state reconstruction without $action should fail") {
      val e = intercept[IllegalStateException] {
        withLogForGoldenTable(s"deltalog-state-reconstruction-without-$action") { log =>
          // snapshot.state is a lazy val; need it to be accessed for the replay to begin
          log.snapshot().getAllFiles
        }
      }
      assert(e.getMessage === DeltaErrors.actionNotFoundException(action, 0).getMessage)
    }
  }

  Seq("protocol", "metadata").foreach { action =>
    test(s"state reconstruction from checkpoint with missing $action should fail") {
      val e = intercept[IllegalStateException] {
        val tblName = s"deltalog-state-reconstruction-from-checkpoint-missing-$action"
        withLogForGoldenTable(tblName) { log =>
          // snapshot.state is a lazy val; need it to be accessed for the replay to begin
          log.snapshot().getAllFiles
        }
      }
      assert (e.getMessage === DeltaErrors.actionNotFoundException(action, 10).getMessage)
    }
  }
}
