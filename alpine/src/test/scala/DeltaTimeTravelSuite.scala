package test.scala

import java.io.File

import scala.concurrent.duration._
import scala.language.implicitConversions

import main.scala.{Snapshot => AlpineSnapshot, DeltaLog => DeltaLogAlpine}
import main.scala.exception.{DeltaErrors => AlpineDeltaErrors}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}

class DeltaTimeTravelSuite extends QueryTest
  with SharedSparkSession {
//  import testImplicits._

  private implicit def durationToLong(duration: FiniteDuration): Long = {
    duration.toMillis
  }

  /** Generate commits with the given timestamp in millis. */
  private def generateCommits(location: String, commits: Long*): Unit = {
    val deltaLog = DeltaLog.forTable(spark, location)
    var startVersion = deltaLog.snapshot.version + 1
    commits.foreach { ts =>
      val rangeStart = startVersion * 10
      val rangeEnd = rangeStart + 10
      spark.range(rangeStart, rangeEnd).write.format("delta").mode("append").save(location)
      val file = new File(FileNames.deltaFile(deltaLog.logPath, startVersion).toUri)
      file.setLastModified(ts)
      startVersion += 1
    }
  }

  private def identifierWithVersion(identifier: String, v: Long): String = {
    s"$identifier@v$v"
  }

  test("versionAsOf") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLogAlpine.forTable(hadoopConf, tblLoc)

      def readRowsFromSnapshotFiles(snapshot: AlpineSnapshot): Set[Row] = {
        snapshot.allFiles.map(_.path).flatMap { path =>
          spark.read.format("parquet").load(s"$tblLoc/$path").collect()
        }
      }

      def assertCorrectSnapshot(version: Long, expectedNumRows: Long): Unit = {
        val df = spark.read.format("delta").load(identifierWithVersion(tblLoc, version))
        val snapshot = alpineLog.getSnapshotForVersionAsOf(version)
        checkAnswer(df.groupBy().count(), Row(expectedNumRows))
        assert(readRowsFromSnapshotFiles(snapshot).size == expectedNumRows)
      }

      val start = 1540415658000L
      generateCommits(tblLoc, start, start + 20.minutes, start + 40.minutes)

      assertCorrectSnapshot(0, 10)
      assertCorrectSnapshot(1, 20)
      assertCorrectSnapshot(2, 30)

      // TODO: use more specfic exception? e.g. AnalysisException
      val e = intercept[Exception] {
        alpineLog.getSnapshotForVersionAsOf(3)
      }
      assert(e.getMessage == AlpineDeltaErrors.versionNotExistException(3, 0, 2).getMessage)

      // TODO: reproducible? Delete the log files?
    }
  }
}
