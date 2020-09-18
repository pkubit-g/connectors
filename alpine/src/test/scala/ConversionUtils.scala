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

package test.scala

import main.scala.{CheckpointMetaData => CheckpointMetaDataAlpine}
import main.scala.actions.{AddFile => AddFileAlpine, Metadata => MetadataAlpine, Protocol => ProtocolAlpine}
import org.apache.spark.sql.delta.CheckpointMetaData
import org.apache.spark.sql.delta.actions.{AddFile, Format, Metadata, Protocol}

trait ConversionUtils {

  def convertAddFile(file: AddFileAlpine): AddFile = {
    AddFile(
      file.path,
      file.partitionValues,
      file.size,
      file.modificationTime,
      file.dataChange,
      file.stats,
      file.tags)
  }

  def convertMetaData(metadata: MetadataAlpine): Metadata = {
    Metadata(
      metadata.id,
      metadata.name,
      metadata.description,
      Format(metadata.format.provider, metadata.format.options),
      metadata.schemaString,
      metadata.partitionColumns,
      metadata.configuration,
      metadata.createdTime)
  }

  def convertProtocol(protocol: ProtocolAlpine): Protocol = {
    Protocol(protocol.minReaderVersion, protocol.minWriterVersion)
  }

  def convertCheckpointMetaData(data: CheckpointMetaDataAlpine): CheckpointMetaData = {
    CheckpointMetaData(data.version, data.size, data.parts)
  }
}
