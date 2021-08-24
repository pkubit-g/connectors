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

package io.delta.standalone.internal

import java.util.{Collections, Optional, UUID}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import io.delta.standalone.{CommitConflictChecker, DeltaLog}
import io.delta.standalone.actions.{Action => ActionJ, AddFile => AddFileJ, CommitInfo => CommitInfoJ, Format => FormatJ, Metadata => MetadataJ, Protocol => ProtocolJ, RemoveFile => RemoveFileJ}
import io.delta.standalone.internal.actions.{Action, AddFile, Metadata, Protocol, RemoveFile}
import io.delta.standalone.internal.exception.{ConcurrentAppendException, ConcurrentDeleteReadException, DeltaErrors}
import io.delta.standalone.internal.exception.DeltaErrors.InvalidProtocolVersionException
import io.delta.standalone.internal.util.ConversionUtils
import io.delta.standalone.types.{StringType, StructField, StructType}
import io.delta.standalone.internal.util.TestUtils._
import io.delta.standalone.operations.ManualUpdate
import org.apache.hadoop.conf.Configuration

// scalastyle:off funsuite
import org.scalatest.FunSuite

class OptimisticTransactionSuite extends FunSuite {
  // scalastyle:on funsuite

  val ManualUpdate = new ManualUpdate()

  val A_P1 = "part=1/a"
  val B_P1 = "part=1/b"
  val C_P1 = "part=1/c"
  val C_P2 = "part=2/c"
  val D_P2 = "part=2/d"
  val E_P3 = "part=3/e"
  val F_P3 = "part=3/f"
  val G_P4 = "part=4/g"

  private val addA_P1 = AddFile(A_P1, Map("part" -> "1"), 1, 1, dataChange = true)
  private val addB_P1 = AddFile(B_P1, Map("part" -> "1"), 1, 1, dataChange = true)
  private val addC_P1 = AddFile(C_P1, Map("part" -> "1"), 1, 1, dataChange = true)
  private val addC_P2 = AddFile(C_P2, Map("part" -> "2"), 1, 1, dataChange = true)
  private val addD_P2 = AddFile(D_P2, Map("part" -> "2"), 1, 1, dataChange = true)
  private val addE_P3 = AddFile(E_P3, Map("part" -> "3"), 1, 1, dataChange = true)
  private val addF_P3 = AddFile(F_P3, Map("part" -> "3"), 1, 1, dataChange = true)
  private val addG_P4 = AddFile(G_P4, Map("part" -> "4"), 1, 1, dataChange = true)

  implicit def actionSeqToList[T <: Action](seq: Seq[T]): java.util.List[ActionJ] =
    seq.map(ConversionUtils.convertAction).asJava

  implicit def addFileSeqToList(seq: Seq[AddFile]): java.util.List[AddFileJ] =
    seq.map(ConversionUtils.convertAddFile).asJava

  def withLog(
      actions: Seq[Action],
      partitionCols: Seq[String] = "part" :: Nil)(
      test: DeltaLog => Unit): Unit = {
    val schemaFields = partitionCols.map { p => new StructField(p, new StringType()) }.toArray
    val schema = new StructType(schemaFields)
//  TODO  val metadata = Metadata(partitionColumns = partitionCols, schemaString = schema.json)
    val metadata = Metadata(partitionColumns = partitionCols)

    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(metadata :: Nil, ManualUpdate)
      log.startTransaction().commit(actions, ManualUpdate)

      test(log)
    }
  }

  /**
   * @tparam T expected exception type
   */
  def testMetadata[T <: Throwable : ClassTag](
      metadata: Metadata,
      expectedExceptionMessageSubStr: String): Unit = {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val e1 = intercept[T] {
        log.startTransaction().commit(metadata :: Nil, ManualUpdate)
      }
      assert(e1.getMessage.contains(expectedExceptionMessageSubStr))

      val e2 = intercept[T] {
        log.startTransaction().updateMetadata(ConversionUtils.convertMetadata(metadata))
      }
      assert(e2.getMessage.contains(expectedExceptionMessageSubStr))
    }
  }

  test("basic commit") {
    withLog(addA_P1 :: addB_P1 :: Nil) { log =>
      log.startTransaction().commit(addA_P1.remove :: Nil, ManualUpdate)

      // [...] is what is automatically added my OptimisticTransaction
      // 0 -> metadata [CommitInfo, Protocol]
      // 1 -> addA_P1, addB_P1 [CommitInfo]
      // 2 -> removeA_P1 [CommitInfo]
      val versionLogs = log.getChanges(0).asScala.toList

      assert(versionLogs(0).getActions.asScala.count(_.isInstanceOf[MetadataJ]) == 1)
      assert(versionLogs(0).getActions.asScala.count(_.isInstanceOf[CommitInfoJ]) == 1)
      assert(versionLogs(0).getActions.asScala.count(_.isInstanceOf[ProtocolJ]) == 1)

      assert(versionLogs(1).getActions.asScala.count(_.isInstanceOf[AddFileJ]) == 2)
      assert(versionLogs(1).getActions.asScala.count(_.isInstanceOf[CommitInfoJ]) == 1)

      assert(versionLogs(2).getActions.asScala.count(_.isInstanceOf[RemoveFileJ]) == 1)
      assert(versionLogs(2).getActions.asScala.count(_.isInstanceOf[CommitInfoJ]) == 1)
    }
  }

  test("basic checkpoint") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      (1 to 15).foreach { i =>
        val meta = if (i == 1) Metadata() :: Nil else Nil
        val txn = log.startTransaction()
        val file = AddFile(i.toString, Map.empty, 1, 1, dataChange = true) :: Nil

        val delete: Seq[Action] = if (i > 1) {
          RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
        } else {
          Nil
        }
        txn.commit(meta ++ delete ++ file, ManualUpdate)
      }

      val log2 = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      assert(log2.snapshot.getVersion == 14)
      assert(log2.snapshot.getAllFiles.size == 1)
    }
  }

  test("block concurrent commit when read partition was appended to by concurrent write") {
    withLog(addA_P1 :: addD_P2 :: addE_P3 :: Nil) { log =>
      // TX1 reads only P1
      val tx1 = log.startTransaction()
      tx1.addReadFiles(addA_P1 :: Nil)
      val commitConflictChecker = new CommitConflictChecker {
        override def doesConflict(otherCommitFiles: java.util.List[AddFileJ]): Boolean = {
          otherCommitFiles.asScala.exists(_.getPartitionValues.asScala.exists(_ == ("part" -> "1")))
        }
      }

      // TX2 modifies only P1
      val tx2 = log.startTransaction()
      tx2.commit(addB_P1 :: Nil, ManualUpdate)

      intercept[ConcurrentAppendException] {
        // P1 was modified by TX2. TX1 commit should fail
        tx1.commit(addC_P2 :: addE_P3 :: Nil, ManualUpdate, commitConflictChecker)
      }
    }
  }

  // ALSO readWholeTable should block concurrent delete
  test("block commit when full table read conflicts with delete in any partition") {
    withLog(addA_P1 :: addC_P2 :: Nil) { log =>
      // TX1 reads the whole table (this happens by DEFAULT)
      val tx1 = log.startTransaction()
      tx1.addReadFiles(addA_P1 :: addB_P1 :: Nil)
      // TODO: tx1.readWholeTable

      val tx2 = log.startTransaction()
      tx2.commit(addA_P1.remove :: Nil, ManualUpdate)

      intercept[ConcurrentDeleteReadException] {
        // TX1 read whole table but TX2 concurrently modified partition P1
        tx1.commit(addB_P1.remove :: Nil, ManualUpdate)
      }
    }
  }

  test("prevent improver usage of setIsBlindAppend and readFiles") {
    // setIsBlindAppend -> readFiles
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      txn.setIsBlindAppend()
      val e = intercept[IllegalArgumentException] {
        txn.addReadFiles(addA_P1 :: Nil)
      }
      assert(e.getMessage.contains("setIsBlindAppend has already been called."))
    }

    // readFiles -> setIsBlindAppend
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      txn.addReadFiles(addA_P1 :: Nil)
      val e = intercept[IllegalArgumentException] {
        txn.setIsBlindAppend()
      }
      assert(e.getMessage.contains("addReadFiles has already been called."))
    }

    // setIsBlindAppend -> commit(..., commitConflictChecker)
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      txn.setIsBlindAppend()
      val commitConflictChecker = new CommitConflictChecker {
        override def doesConflict(otherCommitFiles: java.util.List[AddFileJ]): Boolean = {
          // some conflict logic
          true
        }

      }
      val e = intercept[IllegalArgumentException] {
        txn.commit(addA_P1 :: Nil, ManualUpdate, commitConflictChecker)
      }
      assert(e.getMessage.contains("setIsBlindAppend has already been called"))
    }
  }

// FAILS due to error reading, writing, then reading a CommitInfo
//  test("basic - old") {
//    withLogForGoldenTable("snapshot-data0") { log =>
//      withTempDir { dir =>
//        val versionLogsIter = log.getChanges(0, true)
//        assert(versionLogsIter.hasNext, "versionLogsIter for snapshot-data0 should not be empty")
//        val actions = versionLogsIter.next().getActions
//
//        val newTablePath = dir.getCanonicalPath
//        val newLog = DeltaLog.forTable(new Configuration(), newTablePath)
//        val txn = newLog.startTransaction()
//        txn.commit(actions, null)
//      }
//    }
//  }

  test("committing twice in the same transaction should fail") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      txn.commit(Metadata() :: Nil, ManualUpdate)
      val e = intercept[AssertionError] {
        txn.commit(Nil, ManualUpdate)
      }
      assert(e.getMessage.contains("Transaction already committed."))
    }
  }

  test("commits shouldn't have more than one Metadata") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      val e = intercept[AssertionError] {
        txn.commit(Metadata() :: Metadata() :: Nil, ManualUpdate)
      }
      assert(e.getMessage.contains("Cannot change the metadata more than once in a transaction."))
    }
  }

  test("transaction should throw if it cannot read log directory during first commit") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      dir.setReadOnly()

      val txn = log.startTransaction()
      assertThrows[java.io.IOException] {
        txn.commit(Metadata() :: Nil, ManualUpdate)
      }
    }
  }

  test("initial commit without metadata should fail") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      val e = intercept[IllegalStateException] {
        txn.commit(Nil, ManualUpdate)
      }
      assert(e.getMessage == DeltaErrors.metadataAbsentException().getMessage)
    }
  }

  test("prevent invalid Protocol reader/writer versions from being committed") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      val e1 = intercept[IllegalArgumentException] {
        txn.commit(Metadata() :: Protocol(0, 2) :: Nil, ManualUpdate)
      }
      assert(e1.getMessage.contains("The reader version needs to be greater than 0"))

      val e2 = intercept[IllegalArgumentException] {
        txn.commit(Metadata() :: Protocol(1, 0) :: Nil, ManualUpdate)
      }
      assert(e2.getMessage.contains("The writer version needs to be greater than 0"))
    }
  }

  test("prevent protocol downgrades") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(Metadata() :: Protocol(1, 2) :: Nil, ManualUpdate)
      val e = intercept[RuntimeException] {
        log.startTransaction().commit(Protocol(1, 1) :: Nil, ManualUpdate)
      }
      assert(e.getMessage.contains("Protocol version cannot be downgraded"))
    }
  }

  test("AddFile with different partition schema compared to metadata should fail") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(Metadata(partitionColumns = Seq("foo")) :: Nil, ManualUpdate)
      val e = intercept[IllegalStateException] {
        log.startTransaction().commit(addA_P1 :: Nil, ManualUpdate)
      }
      assert(e.getMessage.contains("The AddFile contains partitioning schema different from the " +
        "table's partitioning schema"))
    }
  }

  test("access with protocol too high") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(Metadata() :: Protocol(1, 2) :: Nil, ManualUpdate)
      val txn = log.startTransaction()
      txn.commit(Protocol(1, 3) :: Nil, ManualUpdate)

      val e = intercept[InvalidProtocolVersionException] {
        log.startTransaction().commit(Metadata() :: Nil, ManualUpdate)
      }
      assert(e.getMessage.contains("Delta protocol version (1,3) is too new for this version"))
    }
  }

  test("can't remove from an append-only table") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val metadata = Metadata(configuration = Map("appendOnly" -> "true"))
      log.startTransaction().commit(metadata :: Nil, ManualUpdate)

      val e = intercept[UnsupportedOperationException] {
        log.startTransaction().commit(addA_P1.remove :: Nil, ManualUpdate)
      }
      assert(e.getMessage.contains("This table is configured to only allow appends"))
    }
  }

  test("BROKEN can't update metadata after you've already written data") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      // TODO: write data (use the correct data API after design doc is approved) ...
      val e = intercept[AssertionError] {
        log.startTransaction().updateMetadata(ConversionUtils.convertMetadata(Metadata()))
      }

      assert(e.getMessage.contains("Cannot update the metadata in a transaction that has already " +
        "written data."))
    }
  }

  test("can't update metadata more than once in a transaction") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      txn.updateMetadata(ConversionUtils.convertMetadata(Metadata()))
      val e = intercept[AssertionError] {
        txn.updateMetadata(ConversionUtils.convertMetadata(Metadata()))
      }

      assert(e.getMessage.contains("Cannot change the metadata more than once in a transaction."))
    }
  }

  // TODO: updateMetadata > unenforceable not null constraints are removed from metadata schemaStr

  test("Protocol Action should be automatically added to transaction for new table") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(Metadata() :: Nil, ManualUpdate)
      assert(log.getChanges(0).asScala.next().getActions.contains(new ProtocolJ(1, 2)))
    }
  }

  test("updateMetadata removes Protocol properties from metadata config") {
    // Note: These Protocol properties are not currently exposed to the user. However, they
    //       might be in the future, and nothing is stopping the user now from seeing these
    //       properties in Delta OSS and adding them to the config map here.
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      val metadata = Metadata(configuration = Map(
        Protocol.MIN_READER_VERSION_PROP -> "1",
        Protocol.MIN_WRITER_VERSION_PROP -> "2"
      ))
      txn.updateMetadata(ConversionUtils.convertMetadata(metadata))
      txn.commit(Nil, ManualUpdate)

      val writtenConfig = log.update().getMetadata.getConfiguration
      assert(!writtenConfig.containsKey(Protocol.MIN_READER_VERSION_PROP))
      assert(!writtenConfig.containsKey(Protocol.MIN_WRITER_VERSION_PROP))
    }
  }

  // FUTURE: test that metadata configuration Protocol properties are actually used

  test("can't have duplicate column names") {
    // TODO: just call myStruct.getJson()
    // scalastyle:off
    val schemaStr = """{"type":"struct","fields":[{"name":"col1","type":"integer","nullable":true,"metadata":{}},{"name":"col1","type":"integer","nullable":true,"metadata":{}}]}"""
    // scalastyle:on
    testMetadata[RuntimeException](Metadata(schemaString = schemaStr), "Found duplicate column(s)")
  }

  test("column names (both data and partition) must be acceptable by parquet") {
    // TODO: just call myStruct.getJson()
    // test DATA columns
    // scalastyle:off
    val schemaStr1 = """{"type":"struct","fields":[{"name":"bad;column,name","type":"integer","nullable":true,"metadata":{}}]}"""
    // scalastyle:on
    testMetadata[RuntimeException](Metadata(schemaString = schemaStr1),
      """Attribute name "bad;column,name" contains invalid character(s)""")

    // test PARTITION columns
    testMetadata[RuntimeException](Metadata(partitionColumns = "bad;column,name" :: Nil),
      "Found partition columns having invalid character(s)")
  }

  test("blindAppend commit with a RemoveFile action should fail") {
    withLog(addA_P1 :: Nil) { log =>
      val txn = log.startTransaction()
      txn.setIsBlindAppend()

      val e = intercept[IllegalStateException] {
        txn.commit(addA_P1.remove :: Nil, ManualUpdate)
      }

      assert(e.getMessage == DeltaErrors.invalidBlindAppendException().getMessage)
    }
  }

  // TODO: test doCommit > IllegalStateException

  // TODO: test doCommit > DeltaConcurrentModificationException

  // TODO: test more ConcurrentAppendException

  // TODO: test more ConcurrentDeleteReadException (including readWholeTable)

  // TODO: test checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn with SnapshotIsolation
  // i.e. datachange = false
}
