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

import io.delta.standalone.internal.actions.{Action, AddFile, RemoveFile}
import io.delta.standalone.internal.util.ManualClock
import io.delta.standalone.internal.util.TestUtils._
import io.delta.standalone.operations.ManualUpdate
import org.apache.hadoop.conf.Configuration


// scalastyle:off funsuite
import org.scalatest.FunSuite


// scalastyle:off removeFile
class DeltaRetentionSuite extends FunSuite with DeltaRetentionSuiteBase {
// scalastyle:on funsuite
  val ManualUpdate = new ManualUpdate()

  protected def getLogFiles(dir: File): Seq[File] =
    getDeltaFiles(dir) ++ getCheckpointFiles(dir)

  test("delete expired logs") {
    withTempDir { dir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val conf = new Configuration()
      val log = DeltaLogImpl.forTable(conf, dir.getCanonicalPath, clock)
      val logPath = new File(log.logPath.toUri)
      (1 to 5).foreach { i =>
        val txn = if (i == 1) startTxnWithManualLogCleanup(log) else log.startTransaction()
        val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
        val delete: Seq[Action] = if (i > 1) {
          RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
        } else {
          Nil
        }
        txn.commit(delete ++ file, ManualUpdate)
      }

      val initialFiles = getLogFiles(logPath)
      // Shouldn't clean up, no checkpoint, no expired files
      log.cleanUpExpiredLogs()

      assert(initialFiles === getLogFiles(logPath))

      // TODO: use DeltaConfigs.LOG_RETENTION.defaultValue ?
      clock.advance(log.deltaRetentionMillis + 100)

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
}
