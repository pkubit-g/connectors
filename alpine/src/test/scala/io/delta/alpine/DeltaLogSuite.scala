package io.delta.alpine

import java.io.File

import org.apache.hadoop.conf.Configuration

// scalastyle:off funsuite
import org.scalatest.FunSuite

class DeltaLogSuite extends FunSuite {
  // scalastyle:on funsuite

  private def withLogForGoldenTable(name: String)(func: (DeltaLog, String) => Unit): Unit = {
    val tablePath = new File("golden-tables/src/test/resources/golden", name).getCanonicalPath
    val hadoopConf = new Configuration()
    val alpineLog = DeltaLog.forTable(hadoopConf, tablePath)
    func(alpineLog, tablePath)
  }

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
}
