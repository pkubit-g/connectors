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

import org.apache.hadoop.fs.Path
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * This is a special class to generate golden tables for other projects. Run the following commands
 * to re-generate all golden tables.
 * ```
 *   GENERATE_GOLDEN_TABLES=1 build/sbt 'goldenTables/test'
 * ```
 */
class GoldenTables extends QueryTest with SharedSparkSession {

  private val testOp = ManualUpdate

  private val shouldGenerateGoldenTables = sys.env.contains("GENERATE_GOLDEN_TABLES")

  private lazy val goldenTablePath = {
    val dir = new File("src/test/resources/golden").getCanonicalFile
    require(dir.exists(),
      s"Cannot find $dir. Please run `GENERATE_GOLDEN_TABLES=1 build/sbt 'goldenTables/test'`.")
    dir
  }

  private def generateGoldenTable(name: String)(generator: String => Unit): Unit = {
    if (shouldGenerateGoldenTables) {
      test(name) {
        val tablePath = new File(goldenTablePath, name)
        JavaUtils.deleteRecursively(tablePath)
        generator(tablePath.getCanonicalPath)
      }
    }
  }

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
}
