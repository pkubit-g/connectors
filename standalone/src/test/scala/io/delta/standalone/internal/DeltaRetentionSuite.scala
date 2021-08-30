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

import java.io.File

import io.delta.standalone.internal.actions.{Action, AddFile, Metadata, RemoveFile}
import io.delta.standalone.internal.util.{ConversionUtils, ManualClock}
import io.delta.standalone.internal.util.TestUtils._
import io.delta.standalone.operations.ManualUpdate
import org.apache.hadoop.conf.Configuration


// scalastyle:off funsuite
import org.scalatest.FunSuite


// scalastyle:off removeFile
class DeltaRetentionSuite extends FunSuite with DeltaRetentionSuiteBase {
// scalastyle:on funsuite
  val ManualUpdate = new ManualUpdate()

  val manualLogCleanupMetadata = Metadata(configuration = Map("enableExpiredLogCleanup" -> "false"))

  protected def getLogFiles(dir: File): Seq[File] =
    getDeltaFiles(dir) ++ getCheckpointFiles(dir)

  test("delete expired logs") {
    withTempDir { dir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val log = DeltaLogImpl.forTable(new Configuration(), dir.getCanonicalPath, clock)
      val logPath = new File(log.logPath.toUri)
      (1 to 5).foreach { i =>
        val txn = log.startTransaction()
        val metadata = if (i == 1) manualLogCleanupMetadata :: Nil else Nil
        val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
        val delete: Seq[Action] = if (i > 1) {
          RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
        } else {
          Nil
        }
        txn.commit(metadata ++ delete ++ file, ManualUpdate)
      }

      val initialFiles = getLogFiles(logPath)
      // Shouldn't clean up, no checkpoint, no expired files
      log.cleanUpExpiredLogs()

      assert(initialFiles === getLogFiles(logPath))

      // TODO clock.advance(intervalStringToMillis(DeltaConfigs.LOG_RETENTION.defaultValue) +
      //  intervalStringToMillis("interval 1 day"))
      clock.advance(log.deltaRetentionMillis + 1000*60*60*24) // 1 day

      // Shouldn't clean up, no checkpoint, although all files have expired
      log.cleanUpExpiredLogs()
      assert(initialFiles === getLogFiles(logPath))

      log.checkpoint()

      val expectedFiles = Seq("04.json", "04.checkpoint.parquet")
      // after checkpointing, the files should be cleared
      log.cleanUpExpiredLogs()
      val afterCleanup = getLogFiles(logPath)
      assert(initialFiles !== afterCleanup)
      assert(expectedFiles.forall(suffix => afterCleanup.exists(_.getName.endsWith(suffix))),
        s"${afterCleanup.mkString("\n")}\n didn't contain files with suffixes: $expectedFiles")
    }
  }

  test("delete expired logs 2") {
    withTempDir { dir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val conf = new Configuration()
      val log = DeltaLogImpl.forTable(conf, dir.getCanonicalPath, clock)
      val logPath = new File(log.logPath.toUri)

      // write 000.json to 009.json
      (0 to 9).foreach { i =>
        val txn = log.startTransaction()
        val metadata = if (i == 0) manualLogCleanupMetadata :: Nil else Nil
        txn.commit(metadata :+ AddFile(i.toString, Map.empty, 1, 1, true), ManualUpdate)
      }

      assert(log.update().version == 9)
      assert(getDeltaFiles(logPath).size == 10)
      assert(getCheckpointFiles(logPath).isEmpty)

      // Local filesystem will truncate the logFile last modified timestamps to the nearest second.
      // This allows for contiguous log & checkpoint files to have the same timestamp.
      // e.g. 00.json, 00.checkpoint, 01.json. 01.checkpoint have lastModified time 1630107078000.
      // This breaks assumptions made in [[BufferingLogDeletionIterator]].
      // This will never happen in production, so let's just fix the timestamps
      val now = clock.getTimeMillis()
      getLogFiles(logPath).sortBy(_.getName).zipWithIndex.foreach { case (file, idx) =>
        file.setLastModified(now + 1000 * idx)
      }

      // to expire log files, advance by the retention duration, then another day (since we
      // truncate)
      clock.advance(log.deltaRetentionMillis + 2*1000*60*60*24 + 1000*100)
      // now, 000.json to 009.json have all expired

      // write 010.json and 010.checkpoint
      log.startTransaction().commit(AddFile("10", Map.empty, 1, 1, true) :: Nil, ManualUpdate)

      getLogFiles(logPath)
        .filter(_.getName.contains("10."))
        .foreach(_.setLastModified(clock.getTimeMillis()))

      // Finally, clean up expired logs. this should delete 000.json to 009.json
      log.cleanUpExpiredLogs()

      assert(log.update().version == 10)
      assert(getDeltaFiles(logPath).size == 1)
      assert(getCheckpointFiles(logPath).size == 1)

      val afterAutoCleanup = getLogFiles(logPath)
      val expectedFiles = Seq("10.json", "10.checkpoint.parquet")
      assert(expectedFiles.forall(suffix => afterAutoCleanup.exists(_.getName.endsWith(suffix))),
        s"${afterAutoCleanup.mkString("\n")}\n didn't contain files with suffixes: $expectedFiles")
    }
  }

  test("Can set enableExpiredLogCleanup") {
    withTempDir { tempDir =>
      val log = DeltaLogImpl.forTable(new Configuration(), tempDir.getCanonicalPath)
      log.startTransaction().commit(
        Metadata(configuration = Map("enableExpiredLogCleanup" -> "true")) :: Nil, ManualUpdate)
      assert(log.enableExpiredLogCleanup)

      log.startTransaction().commit(manualLogCleanupMetadata :: Nil, ManualUpdate)
      assert(!log.enableExpiredLogCleanup)

      log.startTransaction().commit(Metadata() :: Nil, ManualUpdate)
      assert(log.enableExpiredLogCleanup)
    }
  }

  test(
    "RemoveFiles persist across checkpoints as tombstones if retention time hasn't expired") {
    withTempDir { tempDir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val log1 = DeltaLogImpl.forTable(new Configuration(), tempDir.getCanonicalPath, clock)

      val txn1 = log1.startTransaction()
      val files1 = (1 to 10).map(f => AddFile(f.toString, Map.empty, 1, 1, true))
      txn1.commit(files1 :+ manualLogCleanupMetadata, ManualUpdate)
      val txn2 = log1.startTransaction()
      val files2 = (1 to 4).map(f => RemoveFile(f.toString, Some(clock.getTimeMillis())))
      txn2.commit(files2, ManualUpdate)
      log1.checkpoint()

      val log2 = DeltaLogImpl.forTable(new Configuration(), tempDir.getCanonicalPath, clock)
      assert(log2.snapshot.tombstonesScala.size === 4)
      assert(log2.snapshot.allFilesScala.size === 6)
    }
  }

  test("RemoveFiles get deleted during checkpoint if retention time has passed") {
    withTempDir { tempDir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val log1 = DeltaLogImpl.forTable(new Configuration(), tempDir.getCanonicalPath, clock)

      val txn = log1.startTransaction()
      val files1 = (1 to 10).map(f => AddFile(f.toString, Map.empty, 1, 1, true))
      txn.commit(files1 :+ manualLogCleanupMetadata, ManualUpdate)
      val txn2 = log1.startTransaction()
      val files2 = (1 to 4).map(f => RemoveFile(f.toString, Some(clock.getTimeMillis())))
      txn2.commit(files2, ManualUpdate)

      // TODO: intervalStringToMillis(DeltaConfigs.TOMBSTONE_RETENTION.defaultValue) + 1000000L)
      clock.advance(log1.tombstoneRetentionMillis  + 1000000L)

      log1.checkpoint()

      val log2 = DeltaLogImpl.forTable(new Configuration(), tempDir.getCanonicalPath, clock)
      assert(log2.snapshot.tombstonesScala.size === 0)
      assert(log2.snapshot.allFilesScala.size === 6)
    }
  }
}
