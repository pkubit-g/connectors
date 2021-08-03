package io.delta.standalone.internal

// scalastyle:off funsuite
import org.scalatest.FunSuite

class OptimisticTransactionSuite extends FunSuite {
  // scalastyle:on funsuite

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
