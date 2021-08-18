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

import io.delta.standalone.{CommitConflictChecker, DeltaLog}
import io.delta.standalone.actions.{Action => ActionJ, AddFile => AddFileJ, Format => FormatJ, Metadata => MetadataJ, RemoveFile => RemoveFileJ}
import io.delta.standalone.internal.exception.ConcurrentAppendException
import io.delta.standalone.types.{IntegerType, StringType, StructField, StructType}
import io.delta.standalone.internal.util.TestUtils._
import org.apache.hadoop.conf.Configuration

// scalastyle:off funsuite
import org.scalatest.FunSuite

class OptimisticTransactionSuite extends FunSuite {
  // scalastyle:on funsuite

//  val schema = new StructType(Array(
//    new StructField("col1", new IntegerType(), true),
//    new StructField("col2", new StringType(), true)))

  val metadata = new MetadataJ(UUID.randomUUID().toString, null, null, new FormatJ(), null,
    Collections.emptyList(), Collections.emptyMap(), Optional.of(100L), new StructType(Array.empty))

  val add1 = new AddFileJ("fake/path/1", Collections.emptyMap(), 100, 100, true, null, null)
  val add2 = new AddFileJ("fake/path/2", Collections.emptyMap(), 100, 100, true, null, null)

  def createAddFileJ(path: String): AddFileJ = {
    new AddFileJ(path, Collections.emptyMap(), 100, 100, true, null, null)
  }

  def createRemoveFileJ(path: String): RemoveFileJ = {
    new RemoveFileJ(path, Optional.of(100L), true, false, null, 0, null)
  }

  implicit def seqToList[T](seq: Seq[T]): java.util.List[T] = seq.asJava

  test("basic") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      val actions = Seq(metadata, add1, add2)
      txn.commit(actions, null)

      val versionLogs = log.getChanges(0).asScala.toList
      val readActions = versionLogs(0).getActions.asScala

      assert(actions.toSet.subsetOf(readActions.toSet))
    }
  }

  test("checkpoint") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      (1 to 15).foreach { i =>
        val meta = if (i == 1) metadata :: Nil else Nil
        val txn = log.startTransaction()
        val file = createAddFileJ(i.toString) :: Nil
        val delete: Seq[ActionJ] = if (i > 1) {
          createRemoveFileJ(i - 1 toString) :: Nil
        } else {
          Nil
        }
        txn.commit(meta ++ delete ++ file, null)
      }

      val log2 = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      assert(log2.snapshot.getVersion == 14)
      assert(log2.snapshot.getAllFiles.size == 1)
    }
  }

  test("block concurrent commit when read partition was appended to by concurrent write") {
    val A_P1 = "part=1/a"
    val B_P1 = "part=1/b"
    val E_P3 = "part=3/e"

    val addA_P1 = new AddFileJ(A_P1, Collections.singletonMap("part", "1"), 1, 1, true, null, null)
    val addB_P1 = new AddFileJ(B_P1, Collections.singletonMap("part", "1"), 1, 1, true, null, null)
    val addE_P3 = new AddFileJ(E_P3, Collections.singletonMap("part", "3"), 1, 1, true, null, null)

    val schema = new StructType(Array(new StructField("part", new StringType, false)))

    val metadata = new MetadataJ(UUID.randomUUID().toString, null, null, new FormatJ(),
      null, "part" :: Nil, Collections.emptyMap(), Optional.of(100L), schema)

    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(metadata :: Nil, null)

      // TX1 only reads P1
      val tx1 = log.startTransaction()
      val commitConflictChecker = new CommitConflictChecker {
        override def doesConflict(otherCommitFiles: java.util.List[AddFileJ]): Boolean = {
          otherCommitFiles.asScala.exists(_.getPartitionValues.asScala.exists(_ == ("part" -> "1")))
        }
      }
      val readFiles = java.util.Arrays.asList(addA_P1)
      tx1.setConflictResolutionMeta(readFiles, commitConflictChecker)

      val tx2 = log.startTransaction()
      tx2.commit(addB_P1 :: Nil, null)

      intercept[ConcurrentAppendException] {
        // P1 was modified by TX2. TX1 commit should fail
        tx1.commit(metadata :: addE_P3 :: Nil, null)
      }
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

  // TODO: test prepareCommit > assert not already committed

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

  // TODO: test commit > DELTA_COMMIT_INFO_ENABLED
  // - commitInfo is actually added to final actions
  // - isBlindAppend == true
  // - isBlindAppend == false
  // - different operation names

  // TODO: test doCommit > IllegalStateException

  // TODO: test doCommit > DeltaConcurrentModificationException
}
