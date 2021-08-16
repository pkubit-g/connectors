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

import java.io.FileNotFoundException

import com.github.mjakubowski84.parquet4s.ParquetWriter
import scala.util.control.NonFatal

import io.delta.standalone.internal.actions.{Action, AddFile, Metadata, SingleAction}
import org.apache.hadoop.fs.Path
import io.delta.standalone.internal.util.JsonUtils
import io.delta.standalone.internal.util.FileNames._
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
 * Records information about a checkpoint.
 *
 * @param version the version of this checkpoint
 * @param size the number of actions in the checkpoint
 * @param parts the number of parts when the checkpoint has multiple parts. None if this is a
 *              singular checkpoint
 */
private[internal] case class CheckpointMetaData(
    version: Long,
    size: Long,
    parts: Option[Int])

/**
 * A class to help with comparing checkpoints with each other, where we may have had concurrent
 * writers that checkpoint with different number of parts.
 */
private[internal] case class CheckpointInstance(
    version: Long,
    numParts: Option[Int]) extends Ordered[CheckpointInstance] {

  /**
   * Due to lexicographic sorting, a version with more parts will appear after a version with
   * less parts during file listing. We use that logic here as well.
   */
  def isEarlierThan(other: CheckpointInstance): Boolean = {
    if (other == CheckpointInstance.MaxValue) return true
    version < other.version ||
      (version == other.version && numParts.forall(_ < other.numParts.getOrElse(1)))
  }

  def isNotLaterThan(other: CheckpointInstance): Boolean = {
    if (other == CheckpointInstance.MaxValue) return true
    version <= other.version
  }

  def getCorrespondingFiles(path: Path): Seq[Path] = {
    assert(this != CheckpointInstance.MaxValue, "Can't get files for CheckpointVersion.MaxValue.")
    numParts match {
      case None => checkpointFileSingular(path, version) :: Nil
      case Some(parts) => checkpointFileWithParts(path, version, parts)
    }
  }

  override def compare(that: CheckpointInstance): Int = {
    if (version == that.version) {
      numParts.getOrElse(1) - that.numParts.getOrElse(1)
    } else {
      // we need to guard against overflow. We just can't return (this - that).toInt
      if (version - that.version < 0) -1 else 1
    }
  }
}

private[internal] object CheckpointInstance {
  def apply(path: Path): CheckpointInstance = {
    CheckpointInstance(checkpointVersion(path), numCheckpointParts(path))
  }

  def apply(metadata: CheckpointMetaData): CheckpointInstance = {
    CheckpointInstance(metadata.version, metadata.parts)
  }

  val MaxValue: CheckpointInstance = CheckpointInstance(-1, None)
}

private[internal] trait Checkpoints {
  self: DeltaLogImpl =>

  /** The path to the file that holds metadata about the most recent checkpoint. */
  val LAST_CHECKPOINT = new Path(logPath, "_last_checkpoint")

  /** Returns information about the most recent checkpoint. */
  def lastCheckpoint: Option[CheckpointMetaData] = {
    loadMetadataFromFile(0)
  }

  /**
   * Creates a checkpoint using snapshotToCheckpoint. By default it uses the current log version.
   */
  def checkpoint(snapshotToCheckpoint: SnapshotImpl): Unit = {
    if (snapshotToCheckpoint.version < 0) {
      // TODO throw DeltaErrors.checkpointNonExistTable(dataPath)
    }
    val checkpointMetaData = Checkpoints.writeCheckpoint(this, snapshotToCheckpoint)
    val json = JsonUtils.toJson(checkpointMetaData)
    store.write(LAST_CHECKPOINT, Iterator(json), overwrite = true)

    // TODO: doLogCleanup()
  }

  /** Loads the checkpoint metadata from the _last_checkpoint file. */
  private def loadMetadataFromFile(tries: Int): Option[CheckpointMetaData] = {
    try {
      val checkpointMetadataJson = store.read(LAST_CHECKPOINT)
      val checkpointMetadata =
        JsonUtils.mapper.readValue[CheckpointMetaData](checkpointMetadataJson.head)
      Some(checkpointMetadata)
    } catch {
      case _: FileNotFoundException =>
        None
      case NonFatal(e) if tries < 3 =>
        // scalastyle:off println
        println(s"Failed to parse $LAST_CHECKPOINT. This may happen if there was an error " +
          "during read operation, or a file appears to be partial. Sleeping and trying again.", e)
        // scalastyle:on println

        Thread.sleep(1000)
        loadMetadataFromFile(tries + 1)
      case NonFatal(e) =>
        // scalastyle:off println
        println(s"$LAST_CHECKPOINT is corrupted. Will search the checkpoint files directly", e)
        // scalastyle:on println
        // Hit a partial file. This could happen on Azure as overwriting _last_checkpoint file is
        // not atomic. We will try to list all files to find the latest checkpoint and restore
        // CheckpointMetaData from it.
        val verifiedCheckpoint = findLastCompleteCheckpoint(CheckpointInstance(-1L, None))
        verifiedCheckpoint.map(manuallyLoadCheckpoint)
    }
  }

  /** Loads the given checkpoint manually to come up with the CheckpointMetaData */
  private def manuallyLoadCheckpoint(cv: CheckpointInstance): CheckpointMetaData = {
    CheckpointMetaData(cv.version, -1L, cv.numParts)
  }

  /**
   * Finds the first verified, complete checkpoint before the given version.
   *
   * @param cv The CheckpointVersion to compare against
   */
  protected def findLastCompleteCheckpoint(cv: CheckpointInstance): Option[CheckpointInstance] = {
    var cur = math.max(cv.version, 0L)
    while (cur >= 0) {
      val checkpoints = store.listFrom(checkpointPrefix(logPath, math.max(0, cur - 1000)))
        .map(_.getPath)
        .filter(isCheckpointFile)
        .map(CheckpointInstance(_))
        .takeWhile(tv => (cur == 0 || tv.version <= cur) && tv.isEarlierThan(cv))
        .toArray
      val lastCheckpoint = getLatestCompleteCheckpointFromList(checkpoints, cv)
      if (lastCheckpoint.isDefined) {
        return lastCheckpoint
      } else {
        cur -= 1000
      }
    }
    None
  }

  /**
   * Given a list of checkpoint files, pick the latest complete checkpoint instance which is not
   * later than `notLaterThan`.
   */
  protected def getLatestCompleteCheckpointFromList(
      instances: Array[CheckpointInstance],
      notLaterThan: CheckpointInstance): Option[CheckpointInstance] = {
    val complete = instances.filter(_.isNotLaterThan(notLaterThan)).groupBy(identity).filter {
      case (CheckpointInstance(_, None), inst) => inst.length == 1
      case (CheckpointInstance(_, Some(parts)), inst) => inst.length == parts
    }
    complete.keys.toArray.sorted.lastOption
  }
}

private[internal] object Checkpoints {
  /**
   * Writes out the contents of a [[Snapshot]] into a checkpoint file that
   * can be used to short-circuit future replays of the log.
   *
   * Returns the checkpoint metadata to be committed to a file. We will use the value
   * in this file as the source of truth of the last valid checkpoint.
   */
   def writeCheckpoint(deltaLog: DeltaLogImpl, snapshot: SnapshotImpl): CheckpointMetaData = {

    // The writing of checkpoints doesn't go through log store, so we need to check with the
    // log store and decide whether to use rename.
    val useRename = deltaLog.store.isPartialWriteVisible(deltaLog.logPath)

    var checkpointSize = 0L
    var numOfAddFiles = 0L

    // Use the string in the closure as Path is not Serializable.
    val path = checkpointFileSingular(snapshot.path, snapshot.version).toString

    // TODO ++ snapshot.removeFiles ++ snapshot.addFiles ++ snapshot.transaction
    // TODO SingleAction instead of Action?
    // do NOT include commitInfo
    // see https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-schema
     val actions: Seq[SingleAction] =
      (Seq(snapshot.protocolScala, snapshot.metadataScala) ++ snapshot.allFilesScala).map(_.wrap)

    val writerOptions = ParquetWriter.Options(
      compressionCodecName = CompressionCodecName.SNAPPY,
      timeZone = snapshot.readTimeZone // TODO: this should just be timeZone
    )
    val writer = ParquetWriter.writer[SingleAction](path, writerOptions)

    try {
      // TODO useRename

      actions.foreach { singleAction =>
        writer.write(singleAction)
        checkpointSize += 1
        if (singleAction.add != null) {
          numOfAddFiles += 1
        }
      }
    } finally {
      writer.close()
    }

    // TODO: more useRename stuff

    if (numOfAddFiles != snapshot.numOfFiles) {
      throw new IllegalStateException(
        "State of the checkpoint doesn't match that of the snapshot.")
    }

    CheckpointMetaData(snapshot.version, checkpointSize, None)
  }
}
