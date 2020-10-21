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
package io.delta.golden

import java.io.File

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.{Action, AddFile, Metadata, Protocol, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * This is a special class to generate golden tables for other projects. Run the following commands
 * to re-generate all golden tables.
 * ```
 * GENERATE_GOLDEN_TABLES=1 build/sbt 'goldenTables/test'
 * ```
 */
class GoldenTables extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val testOp = ManualUpdate

  private val shouldGenerateGoldenTables = sys.env.contains("GENERATE_GOLDEN_TABLES")

  private lazy val goldenTablePath = {
    val dir = new File("src/test/resources/golden").getCanonicalFile
    require(dir.exists(),
      s"Cannot find $dir. Please run `GENERATE_GOLDEN_TABLES=1 build/sbt 'goldenTables/test'`.")
    dir
  }

  private def copyDir(src: String, dest: String): Unit = {
    FileUtils.copyDirectory(createGoldenTableFile(src), createGoldenTableFile(dest))
  }

  private def createGoldenTableFile(name: String): File = new File(goldenTablePath, name)

  private def generateGoldenTable(name: String)(generator: String => Unit): Unit = {
    if (shouldGenerateGoldenTables) {
      test(name) {
        val tablePath = createGoldenTableFile(name)
        JavaUtils.deleteRecursively(tablePath)
        generator(tablePath.getCanonicalPath)
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // io.delta.alpine.internal.DeltaLogSuite
  ///////////////////////////////////////////////////////////////////////////

  /** TEST: DeltaLogSuite > checkpoint */
  generateGoldenTable("checkpoint") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    (1 to 15).foreach { i =>
      val txn = log.startTransaction()
      val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
      val delete: Seq[Action] = if (i > 1) {
        RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
      } else {
        Nil
      }
      txn.commit(delete ++ file, testOp)
    }
  }

  /** TEST: DeltaLogSuite > snapshot */
  private def writeData(data: Seq[(Int, String)], mode: String, tablePath: String): Unit = {
    data.toDS
      .toDF("col1", "col2")
      .write
      .mode(mode)
      .format("delta")
      .save(tablePath)
  }

  generateGoldenTable("snapshot-data0") { tablePath =>
    writeData((0 until 10).map(x => (x, s"data-0-$x")), "append", tablePath)
  }

  generateGoldenTable("snapshot-data1") { tablePath =>
    copyDir("snapshot-data0", "snapshot-data1")
    writeData((0 until 10).map(x => (x, s"data-1-$x")), "append", tablePath)
  }

  generateGoldenTable("snapshot-data2") { tablePath =>
    copyDir("snapshot-data1", "snapshot-data2")
    writeData((0 until 10).map(x => (x, s"data-2-$x")), "overwrite", tablePath)
  }

  generateGoldenTable("snapshot-data3") { tablePath =>
    copyDir("snapshot-data2", "snapshot-data3")
    writeData((0 until 20).map(x => (x, s"data-3-$x")), "append", tablePath)
  }

  generateGoldenTable("snapshot-data2-deleted") { tablePath =>
    copyDir("snapshot-data3", "snapshot-data2-deleted")
    DeltaTable.forPath(spark, tablePath).delete("col2 like 'data-2-%'")
  }

  generateGoldenTable("snapshot-repartitioned") { tablePath =>
    copyDir("snapshot-data2-deleted", "snapshot-repartitioned")
    spark.read
      .format("delta")
      .load(tablePath)
      .repartition(2)
      .write
      .option("dataChange", "false")
      .format("delta")
      .mode("overwrite")
      .save(tablePath)
  }

  generateGoldenTable("snapshot-vacuumed") { tablePath =>
    copyDir("snapshot-repartitioned", "snapshot-vacuumed")
    withSQLConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
      DeltaTable.forPath(spark, tablePath).vacuum(0.0)
    }
  }

  /** TEST: DeltaLogSuite > SC-8078: update deleted directory */
  generateGoldenTable("update-deleted-directory") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    val txn = log.startTransaction()
    val files = (1 to 10).map(f => AddFile(f.toString, Map.empty, 1, 1, true))
    txn.commit(files, testOp)
    log.checkpoint()
  }

  /** TEST: DeltaLogSuite > update shouldn't pick up delta files earlier than checkpoint */
  // TODO

  /** TEST: DeltaLogSuite > handle corrupted '_last_checkpoint' file */
  generateGoldenTable("corrupted-last-checkpoint") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    val checkpointInterval = log.checkpointInterval
    for (f <- 0 to checkpointInterval) {
      val txn = log.startTransaction()
      txn.commit(AddFile(f.toString, Map.empty, 1, 1, true) :: Nil, testOp)
    }
  }

  /** TEST: DeltaLogSuite > paths should be canonicalized */
  {
    def helper(scheme: String, path: String, tableSuffix: String): Unit = {
      generateGoldenTable(s"canonicalized-paths-$tableSuffix") { tablePath =>
        val log = DeltaLog.forTable(spark, new Path(tablePath))
        new File(log.logPath.toUri).mkdirs()

        val add = AddFile(path, Map.empty, 100L, 10L, dataChange = true)
        val rm = RemoveFile(s"$scheme$path", Some(200L), dataChange = false)

        log.store.write(
          FileNames.deltaFile(log.logPath, 0L),
          Iterator(Protocol(), Metadata(), add).map(a => JsonUtils.toJson(a.wrap)))
        log.store.write(
          FileNames.deltaFile(log.logPath, 1L),
          Iterator(JsonUtils.toJson(rm.wrap)))
      }
    }

    // normal characters
    helper("file:", "/some/unqualified/absolute/path", "normal-a")
    helper("file://", "/some/unqualified/absolute/path", "normal-b")

    // special characters
    helper("file:", new Path("/some/unqualified/with space/p@#h").toUri.toString, "special-a")
    helper("file://", new Path("/some/unqualified/with space/p@#h").toUri.toString, "special-b")
  }

  /** TEST: DeltaLogSuite > delete and re-add the same file in different transactions */
  generateGoldenTable(s"delete-re-add-same-file-different-transactions") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    assert(new File(log.logPath.toUri).mkdirs())

    val add1 = AddFile("foo", Map.empty, 1L, 1600000000000L, dataChange = true)
    log.startTransaction().commit(add1 :: Nil, testOp)

    val rm = add1.remove
    log.startTransaction().commit(rm :: Nil, testOp)

    val add2 = AddFile("foo", Map.empty, 1L, 1700000000000L, dataChange = true)
    log.startTransaction().commit(add2 :: Nil, testOp)

    // Add a new transaction to replay logs using the previous snapshot. If it contained
    // AddFile("foo") and RemoveFile("foo"), "foo" would get removed and fail this test.
    val otherAdd = AddFile("bar", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
    log.startTransaction().commit(otherAdd :: Nil, testOp)
  }

  /** TEST: DeltaLogSuite > error - versions not contiguous */
  generateGoldenTable("versions-not-contiguous") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    assert(new File(log.logPath.toUri).mkdirs())

    val add1 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
    log.startTransaction().commit(add1 :: Nil, testOp)

    val add2 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
    log.startTransaction().commit(add2 :: Nil, testOp)

    val add3 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
    log.startTransaction().commit(add3 :: Nil, testOp)

    new File(new Path(log.logPath, "00000000000000000001.json").toUri).delete()
  }

  ///////////////////////////////////////////////////////////////////////////
  // io.delta.alpine.internal.ReadOnlyLogStoreSuite
  ///////////////////////////////////////////////////////////////////////////

  /** TEST: ReadOnlyLogStoreSuite > read */
  generateGoldenTable("log-store-read") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    assert(new File(log.logPath.toUri).mkdirs())

    val deltas = Seq(0, 1).map(i => new File(tablePath, i.toString)).map(_.getCanonicalPath)
    log.store.write(deltas.head, Iterator("zero", "none"))
    log.store.write(deltas(1), Iterator("one"))
  }

  /** TEST: ReadOnlyLogStoreSuite > listFrom */
  generateGoldenTable("log-store-listFrom") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    assert(new File(log.logPath.toUri).mkdirs())

    val deltas = Seq(0, 1, 2, 3, 4)
      .map(i => new File(tablePath, i.toString))
      .map(_.getCanonicalPath)

    log.store.write(deltas(1), Iterator("zero"))
    log.store.write(deltas(2), Iterator("one"))
    log.store.write(deltas(3), Iterator("two"))
  }
}
