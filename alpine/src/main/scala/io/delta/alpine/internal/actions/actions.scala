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

package io.delta.alpine.internal.actions

import java.net.URI

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude, JsonRawValue}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.delta.alpine.internal.util.{DataTypeParser, JsonUtils}
import io.delta.alpine.types.StructType

private[internal] object Action {
  val readerVersion = 1
  val writerVersion = 2
  val protocolVersion: Protocol = Protocol(readerVersion, writerVersion)

  def fromJson(json: String): Action = {
    JsonUtils.mapper.readValue[SingleAction](json).unwrap
  }
}

private[internal] sealed trait Action {
  def wrap: SingleAction

  def json: String = JsonUtils.toJson(wrap)
}

private[internal] case class Protocol(
    minReaderVersion: Int = Action.readerVersion,
    minWriterVersion: Int = Action.writerVersion) extends Action {
  override def wrap: SingleAction = SingleAction(protocol = this)

  @JsonIgnore
  def simpleString: String = s"($minReaderVersion,$minWriterVersion)"
}

private[internal] sealed trait FileAction extends Action {
  val path: String
  val dataChange: Boolean
  @JsonIgnore
  lazy val pathAsUri: URI = new URI(path)
}

private[internal] case class AddFile(
    path: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    modificationTime: Long,
    dataChange: Boolean,
    @JsonRawValue
    stats: String = null,
    tags: Map[String, String] = null) extends FileAction {
  require(path.nonEmpty)

  override def wrap: SingleAction = SingleAction(add = this)

  def remove: RemoveFile = removeWithTimestamp()

  def removeWithTimestamp(
    timestamp: Long = System.currentTimeMillis(),
    dataChange: Boolean = true): RemoveFile = {
    // scalastyle:off
    RemoveFile(path, Some(timestamp), dataChange)
    // scalastyle:on
  }
}

private[internal] case class RemoveFile(
    path: String,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    deletionTimestamp: Option[Long],
    dataChange: Boolean = true) extends FileAction {
  override def wrap: SingleAction = SingleAction(remove = this)

  @JsonIgnore
  val delTimestamp: Long = deletionTimestamp.getOrElse(0L)
}

private[internal] case class Format(
    provider: String = "parquet",
    options: Map[String, String] = Map.empty)

private[internal] case class Metadata(
    id: String = java.util.UUID.randomUUID().toString,
    name: String = null,
    description: String = null,
    format: Format = Format(),
    schemaString: String = null,
    partitionColumns: Seq[String] = Nil,
    configuration: Map[String, String] = Map.empty,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    createdTime: Option[Long] = Some(System.currentTimeMillis())) extends Action {

  /** Returns the schema as a [[StructType]] */
  @JsonIgnore
  lazy val schema: StructType =
    Option(schemaString).map { s =>
      DataTypeParser.fromJson(s).asInstanceOf[StructType]
    }.getOrElse(new StructType(Array.empty))

  override def wrap: SingleAction = SingleAction(metaData = this)
}

private[internal] trait CommitMarker {
  /** Get the timestamp of the commit as millis after the epoch. */
  def getTimestamp: Long
  /** Return a copy object of this object with the given timestamp. */
  def withTimestamp(timestamp: Long): CommitMarker
  /** Get the version of the commit. */
  def getVersion: Long
}

private[internal] case class SingleAction(
    add: AddFile = null,
    remove: RemoveFile = null,
    metaData: Metadata = null,
    protocol: Protocol = null) {

  def unwrap: Action = {
    if (add != null) {
      add
    } else if (remove != null) {
      remove
    } else if (metaData != null) {
      metaData
    } else if (protocol != null) {
      protocol
    } else {
      null
    }
  }
}