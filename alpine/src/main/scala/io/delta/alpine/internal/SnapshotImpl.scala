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

package io.delta.alpine.internal

import java.net.URI

import collection.JavaConverters._

import com.github.mjakubowski84.parquet4s.ParquetReader
import io.delta.alpine
import io.delta.alpine.internal.actions._
import io.delta.alpine.{DeltaLog, Snapshot}
import io.delta.alpine.data.{CloseableIterator, RowRecord => RowParquetRecordJ}
import io.delta.alpine.internal.data.CloseableParquetDataIterator
import io.delta.alpine.internal.exception.DeltaErrors
import io.delta.alpine.internal.sources.AlpineHadoopConf
import io.delta.alpine.internal.util.{ConversionUtils, FileNames, JsonUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

private[internal] class SnapshotImpl(
    val hadoopConf: Configuration,
    val path: Path,
    val version: Long,
    val logSegment: LogSegment,
    val deltaLog: DeltaLogImpl,
    val timestamp: Long) extends Snapshot {

  import SnapshotImpl._

  ///////////////////////////////////////////////////////////////////////////
  // Public API Methods
  ///////////////////////////////////////////////////////////////////////////

  override def getAllFiles: java.util.List[alpine.actions.AddFile] =
    state.activeFiles.values.map(ConversionUtils.convertAddFile).toList.asJava

  override def getNumOfFiles: Int = state.activeFiles.size

  override def getMetadata: alpine.actions.Metadata =
    ConversionUtils.convertMetadata(state.metadata)

  override def getPath: Path = path
  override def getVersion: Long = version
  override def getDeltaLog: DeltaLog = deltaLog
  override def getTimestamp: Long = timestamp

  override def open(): CloseableIterator[RowParquetRecordJ] =
    CloseableParquetDataIterator(
      allFilesScala
        .map(_.path)
        .map(FileNames.absolutePath(deltaLog.dataPath, _).toString),
      getMetadata.getSchema,
      hadoopConf.get(AlpineHadoopConf.PARQUET_DATA_TIME_ZONE_ID))

  ///////////////////////////////////////////////////////////////////////////
  // Internal-Only Methods
  ///////////////////////////////////////////////////////////////////////////

  def allFilesScala: Seq[AddFile] = state.activeFiles.values.toSeq
  def protocolScala: Protocol = state.protocol
  def metadataScala: Metadata = state.metadata

  // TODO: remove unused methods
  def sizeInBytes: Long = state.sizeInBytes
  def numOfFiles: Long = state.numOfFiles
  def numOfMetadata: Long = state.numOfMetadata
  def numOfProtocol: Long = state.numOfProtocol

  private def load(paths: Seq[Path]): Seq[SingleAction] = {
    paths.map(_.toString).sortWith(_ < _).par.flatMap { path =>
      if (path.endsWith("json")) {
        deltaLog.store.read(path).map { line =>
          JsonUtils.mapper.readValue[SingleAction](line)
        }
      } else if (path.endsWith("parquet")) {
        ParquetReader.read[SingleAction](path).toSeq
      } else Seq.empty[SingleAction]
    }.toList
  }

  /**
   * Computes some statistics around the transaction log, therefore on the actions made on this
   * Delta table.
   */
  protected lazy val state: State = {
    val logPathURI = path.toUri
    val replay = new InMemoryLogReplay(hadoopConf)
    val files = (logSegment.deltas ++ logSegment.checkpoints).map(_.getPath)

    // assert log belongs to table
    files.foreach { f =>
      if (f.toString.isEmpty || f.getParent != new Path(logPathURI)) {
        // scalastyle:off throwerror
        throw new AssertionError(s"File (${f.toString}) doesn't belong in the " +
          s"transaction log at $logPathURI. Please contact Databricks Support.")
        // scalastyle:on throwerror
      }
    }

    val actions = load(files).map(_.unwrap)

    replay.append(0, actions.iterator)

    if (null == replay.currentProtocolVersion) {
      throw DeltaErrors.actionNotFoundException("protocol", version)
    }
    if (null == replay.currentMetaData) {
      throw DeltaErrors.actionNotFoundException("metadata", version)
    }

    State(
      replay.currentProtocolVersion,
      replay.currentMetaData,
      replay.getActiveFiles,
      replay.sizeInBytes,
      replay.getActiveFiles.size,
      replay.numMetadata,
      replay.numProtocol
    )
  }

  /**
   * Asserts that the client is up to date with the protocol and allowed
   * to read the table that is using this Snapshot's `protocol`.
   */
  def assertProtocolRead(): Unit = {
    if (null != protocolScala) {
      val clientVersion = Action.readerVersion
      val tblVersion = protocolScala.minReaderVersion

      if (clientVersion < tblVersion) {
        throw DeltaErrors.invalidProtocolVersionException(clientVersion, tblVersion)
      }
    }
  }

  /** Complete initialization by checking protocol version. */
  assertProtocolRead()
}

object SnapshotImpl {
  /** Canonicalize the paths for Actions. */
  def canonicalizePath(path: String, hadoopConf: Configuration): String = {
    val hadoopPath = new Path(new URI(path))
    if (hadoopPath.isAbsoluteAndSchemeAuthorityNull) {
      val fs = FileSystem.get(hadoopConf)
      fs.makeQualified(hadoopPath).toUri.toString
    } else {
      // return untouched if it is a relative path or is already fully qualified
      hadoopPath.toUri.toString
    }
  }

  case class State(
      protocol: Protocol,
      metadata: Metadata,
      activeFiles: scala.collection.immutable.Map[URI, AddFile],
      sizeInBytes: Long,
      numOfFiles: Long,
      numOfMetadata: Long,
      numOfProtocol: Long)
}

class InitialSnapshotImpl(
    override val hadoopConf: Configuration,
    val logPath: Path,
    override val deltaLog: DeltaLogImpl,
    val metadata: Metadata)
  extends SnapshotImpl(hadoopConf, logPath, -1, LogSegment.empty(logPath), deltaLog, -1) {

  def this(hadoopConf: Configuration, logPath: Path, deltaLog: DeltaLogImpl) = this(
    hadoopConf,
    logPath,
    deltaLog,
    Metadata() // TODO: SparkSession.active.sessionState.conf ?
  )

  override lazy val state: SnapshotImpl.State = {
    SnapshotImpl.State(
      Protocol(),
      metadata,
      Map.empty[URI, AddFile],
      0L, 0L, 1L, 1L)
  }
}
