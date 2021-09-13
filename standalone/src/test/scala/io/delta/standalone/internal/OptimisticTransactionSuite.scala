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

import io.delta.standalone.{DeltaLog, Operation}
import io.delta.standalone.actions.{Action => ActionJ, AddFile => AddFileJ, CommitInfo => CommitInfoJ, Format => FormatJ, Metadata => MetadataJ, Protocol => ProtocolJ, RemoveFile => RemoveFileJ}
import io.delta.standalone.internal.actions._
import io.delta.standalone.internal.util.ConversionUtils
import io.delta.standalone.types.{IntegerType, StringType, StructField, StructType}
import io.delta.standalone.internal.util.TestUtils._
import org.apache.hadoop.conf.Configuration

// scalastyle:off funsuite
import org.scalatest.FunSuite

class OptimisticTransactionSuite extends FunSuite {
  // scalastyle:on funsuite
  val writerId = "test-writer-id"
  val manualUpdate = new Operation(Operation.MANUAL_UPDATE)

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

  def createAddFileJ(path: String): AddFileJ = {
    new AddFileJ(path, Collections.emptyMap(), 100, 100, true, null, null)
  }

  def createRemoveFileJ(path: String): RemoveFileJ = {
    new RemoveFileJ(path, Optional.of(100L), true, false, null, 0, null)
  }

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
      log.startTransaction().commit(metadata :: Nil, manualUpdate, writerId)
      log.startTransaction().commit(actions, manualUpdate, writerId)

      test(log)
    }
  }

  test("basic commit") {
    withLog(addA_P1 :: addB_P1 :: Nil) { log =>
      log.startTransaction().commit(addA_P1.remove :: Nil, manualUpdate, writerId)

      // [...] is what is automatically added by OptimisticTransaction
      // 0 -> metadata [CommitInfo, Protocol]
      // 1 -> addA_P1, addB_P1 [CommitInfo]
      // 2 -> removeA_P1 [CommitInfo]
      val versionLogs = log.getChanges(0, true).asScala.toList

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
        txn.commit(meta ++ delete ++ file, manualUpdate, writerId)
      }

      val log2 = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      assert(log2.snapshot.getVersion == 14)
      assert(log2.snapshot.getAllFiles.size == 1)
    }
  }

  // TODO: test prepareCommit > assert not already committed
  test("committing twice in the same transaction should fail") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      txn.commit(Metadata() :: Nil, manualUpdate, writerId)
      val e = intercept[AssertionError] {
        txn.commit(Nil, manualUpdate, writerId)
      }
      assert(e.getMessage.contains("Transaction already committed."))
    }
  }

  // TODO: test prepareCommit > assert user didn't commit a CommitInfo

  // TODO: test prepareCommit > have more than 1 Metadata in transaction

  // TODO: test prepareCommit > 1st commit & ensureLogDirectoryExist throws

  // TODO: test prepareCommit > 1st commit & commitValidationEnabled & metadataAbsentException

  // TODO: test prepareCommit > 1st commit & !commitValidationEnabled & no metadataAbsentException

  // TODO: test prepareCommit > protocolDowngradeException (reader)

  // TODO: test prepareCommit > protocolDowngradeException (writer)

  // TODO: test prepareCommit > commitValidationEnabled & addFilePartitioningMismatchException

  // TODO: test prepareCommit > !commitValidationEnabled & no addFilePartitioningMismatchException

  // TODO: test prepareCommit > assertProtocolWrite

  // TODO: test prepareCommit > assertRemovable

  // TODO: test verifyNewMetadata > SchemaMergingUtils.checkColumnNameDuplication

  // TODO: test verifyNewMetadata > SchemaUtils.checkFieldNames(dataSchema) > ParquetSchemaConverter

  // TODO: test verifyNewMetadata > SchemaUtils.checkFieldNames(dataSchema) > invalidColumnName

  // TODO: test verifyNewMetadata > ...checkFieldNames(partitionColumns) > ParquetSchemaConverter

  // TODO: test verifyNewMetadata > ...checkFieldNames(partitionColumns) > invalidColumnName

  // TODO: test verifyNewMetadata > Protocol.checkProtocolRequirements

  // TODO: test commit
  // - commitInfo is actually added to final actions
  // - isBlindAppend == true
  // - isBlindAppend == false
  // - different operation names

  // TODO: test doCommit > IllegalStateException

  // TODO: test doCommit > DeltaConcurrentModificationException

  // TODO: test more ConcurrentAppendException

  // TODO: test more ConcurrentDeleteReadException (including readWholeTable)

  // TODO: test checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn with SnapshotIsolation
  // i.e. datachange = false
}
