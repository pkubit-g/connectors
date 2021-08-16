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

import io.delta.standalone.internal.actions.{Action, AddFile, Metadata}
import io.delta.standalone.CommitConflictChecker
import io.delta.standalone.internal.util.FileNames

/**
 * A class representing different attributes of current transaction needed for conflict detection.
 *
 * @param externalConflictChecker - TODO
 * @param readFiles - specific files that have been seen by the transaction
 * @param readWholeTable - whether the whole table was read during the transaction
 * @param readAppIds - appIds that have been seen by the transaction
 * @param metadata - table metadata for the transaction
 * @param actions - delta log actions that the transaction wants to commit
 * @param deltaLog - [[DeltaLogImpl]] corresponding to the target table
 */
private[internal] case class CurrentTransactionInfo(
    externalConflictChecker: CommitConflictChecker,
    readFiles: Set[AddFile],
    readWholeTable: Boolean,
    readAppIds: Set[String],
    metadata: Metadata,
    actions: Seq[Action],
    deltaLog: DeltaLogImpl)

/**
 * Summary of the Winning commit against which we want to check the conflict
 * @param actions - delta log actions committed by the winning commit
 * @param commitVersion - winning commit version
 */
private[internal] case class WinningCommitSummary(actions: Seq[Action], commitVersion: Long) {

}

private[internal] class ConflictChecker(
    currentTransactionInfo: CurrentTransactionInfo,
    winningCommitVersion: Long,
    isolationLevel: IsolationLevel) {

  private val winningCommitSummary: WinningCommitSummary = createWinningCommitSummary()

  def checkConflicts(): Unit = {
//    TODO checkProtocolCompatibility()
//    TODO checkNoMetadataUpdates()
    checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn()
    checkForDeletedFilesAgainstCurrentTxnReadFiles()
//    TODO checkForDeletedFilesAgainstCurrentTxnDeletedFiles()
//    TODO checkForUpdatedApplicationTransactionIdsThatCurrentTxnDependsOn()
  }

  /**
   * Initializes [[WinningCommitSummary]] for the already committed
   * transaction (winning transaction).
   */
  private def createWinningCommitSummary(): WinningCommitSummary = {
      val deltaLog = currentTransactionInfo.deltaLog
      val winningCommitActions = deltaLog.store.read(
        FileNames.deltaFile(deltaLog.logPath, winningCommitVersion)).map(Action.fromJson)
      WinningCommitSummary(winningCommitActions, winningCommitVersion)
  }

  /**
   * Check if the new files added by the already committed transactions should have been read by
   * the current transaction.
   */
  def checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn(): Unit = {

  }

  /**
   * Check if [[RemoveFile]] actions added by already committed transactions conflicts with files
   * read by the current transaction.
   */
  def checkForDeletedFilesAgainstCurrentTxnReadFiles(): Unit = {

  }
}
