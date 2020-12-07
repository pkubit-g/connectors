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

package io.delta.standalone.internal.util

import java.util.{Optional => OptionalJ}

import collection.JavaConverters._

import io.delta.standalone.actions.{AddFile => AddFileJ, CommitInfo => CommitInfoJ, Format => FormatJ, JobInfo => JobInfoJ, Metadata => MetadataJ}
import io.delta.standalone.internal.actions.{AddFile, CommitInfo, Format, JobInfo, Metadata}

/**
 * Provide helper methods to convert from Scala to Java types.
 */
private[internal] object ConversionUtils {

  private implicit def convertOption[ScalaT, JavaT](opt: Option[ScalaT]): OptionalJ[JavaT] = {
    OptionalJ.ofNullable(opt.getOrElse(null).asInstanceOf[JavaT])
  }

  /**
   * Convert an [[AddFile]] (Scala) to an [[AddFileJ]] (Java)
   */
  def convertAddFile(internal: AddFile): AddFileJ = {
    new AddFileJ(
      internal.path,
      internal.partitionValues.asJava,
      internal.size,
      internal.modificationTime,
      internal.dataChange,
      internal.stats,
      internal.tags.asJava)
  }

  /**
   * Convert a [[Metadata]] (Scala) to a [[MetadataJ]] (Java)
   */
  def convertMetadata(internal: Metadata): MetadataJ = {
    new MetadataJ(
      internal.id,
      internal.name,
      internal.description,
      convertFormat(internal.format),
      internal.schemaString,
      internal.partitionColumns.toList.asJava,
      internal.configuration.asJava,
      internal.createdTime,
      internal.schema)
  }

  /**
   * Convert a [[Format]] (Scala) to a [[FormatJ]] (Java)
   */
  def convertFormat(internal: Format): FormatJ = {
    new FormatJ(internal.provider, internal.options.asJava)
  }

  /**
   * Convert a [[CommitInfo]] (Scala) to a [[CommitInfoJ]] (Java)
   */
  def convertCommitInfo(internal: CommitInfo): CommitInfoJ = {
    val notebookIdOpt: OptionalJ[String] = if (internal.notebook.isDefined) {
      OptionalJ.of(internal.notebook.get.notebookId) }
    else {
      OptionalJ.empty()
    }

    val jobInfoOpt: OptionalJ[JobInfoJ] = if (internal.job.isDefined) {
      OptionalJ.of(convertJobInfo(internal.job.get))
    } else {
      OptionalJ.empty()
    }

    new CommitInfoJ(
      internal.version,
      internal.timestamp,
      internal.userId,
      internal.userName,
      internal.operation,
      internal.operationParameters.asJava,
      jobInfoOpt,
      notebookIdOpt,
      internal.clusterId,
      internal.readVersion,
      internal.isolationLevel,
      internal.isBlindAppend,
      internal.operationMetrics,
      internal.userMetadata
    )
  }

  /**
   * Convert a [[JobInfo]] (Scala) to a [[JobInfoJ]] (Java)
   */
  def convertJobInfo(internal: JobInfo): JobInfoJ = {
    new JobInfoJ(
      internal.jobId,
      internal.jobName,
      internal.runId,
      internal.jobOwnerId,
      internal.triggerType)
  }
}
