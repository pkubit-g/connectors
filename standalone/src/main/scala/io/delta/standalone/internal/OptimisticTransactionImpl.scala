package io.delta.standalone.internal

import java.util

import io.delta.standalone.{OptimisticTransaction, Action => ActionJ}
import io.delta.standalone.operations.{Operation => OperationJ}
import io.delta.standalone.internal.actions.{Action, CommitInfo}
import io.delta.standalone.internal.sources.StandaloneHadoopConf

private[internal] class OptimisticTransactionImpl(
    deltaLog: DeltaLogImpl,
    snapshot: SnapshotImpl) extends OptimisticTransaction {

  override def commit(actionsJ: util.List[ActionJ]): Long =
    commit(actionsJ, Option.empty[DeltaOperations.Operation])

  override def commit(actionsJ: util.List[ActionJ], opJ: OperationJ): Long = {
    val op: DeltaOperations.Operation = null // convert opJ to scala
    commit(actionsJ, Some(op))
  }

  private def commit(
      actionsJ: util.List[ActionJ],
      op: Option[DeltaOperations.Operation]): Long = {
    val actions = null // TODO convert actionsJ to scala

    val version = try {
      // Try to commit at the next version.
      var finalActions = prepareCommit(actions)

      if (deltaLog.hadoopConf.getBoolean(StandaloneHadoopConf.DELTA_COMMIT_INFO_ENABLED, defaultValue = true)) {
        val commitInfo = CommitInfo.empty() // TODO
        finalActions = finalActions +: commitInfo // prepend
      }

      val commitVersion = doCommitRetryIteratively(
        snapshot.version + 1,
        finalActions)

      commitVersion
    } catch {
      case _ => null // TODO
    }

    version

    0L
  }

  /**
   * Prepare for a commit by doing all necessary pre-commit checks and modifications to the actions.
   * @return The finalized set of actions.
   */
  private def prepareCommit(actions: Seq[Action]): Seq[Action] = {
    null
  }

  /**
   * Commit `actions` using `attemptVersion` version number. If there are any conflicts that are
   * found, we will retry a fixed number of times.
   *
   * @return the real version that was committed
   */
  private def doCommitRetryIteratively(attemptVersion: Long, actions: Seq[Action]): Long = {

  }

  /**
   * Commit `actions` using `attemptVersion` version number. Throws a FileAlreadyExistsException
   * if any conflicts are detected.
   *
   * @return the real version that was committed.
   */
  private def doCommit(attemptVersion: Long, actions: Seq[Action], attemptNumber: Int): Long = {

    attemptVersion
  }
}
