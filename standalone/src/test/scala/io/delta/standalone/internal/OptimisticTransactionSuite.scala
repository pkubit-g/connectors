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

import io.delta.standalone.internal.util.GoldenTableUtils._
import io.delta.standalone.internal.util.TestUtils._
import io.delta.standalone.DeltaLog
import org.apache.hadoop.conf.Configuration

// scalastyle:off funsuite
import org.scalatest.FunSuite

class OptimisticTransactionSuite extends FunSuite {
  // scalastyle:on funsuite

  test("basic") {
    withLogForGoldenTable("snapshot-data0") { log =>
      withTempDir { dir =>
        val versionLogsIter = log.getChanges(0, true)
        assert(versionLogsIter.hasNext, "versionLogsIter for snapshot-data0 should not be empty")

        val newTablePath = dir.getCanonicalPath
        val newLog = DeltaLog.forTable(new Configuration(), newTablePath)
        val txn = newLog.startTransaction()

        val actions = versionLogsIter.next().getActions
        txn.commit(actions)
      }
    }
  }

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
