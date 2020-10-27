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

import collection.JavaConverters._

import io.delta.standalone.actions.{AddFile => AddFileJ, Format => FormatJ, Metadata => MetadataJ}
import io.delta.standalone.internal.actions.{AddFile, Format, Metadata}


private[internal] object ConversionUtils {
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

  def convertMetadata(internal: Metadata): MetadataJ = {
    new MetadataJ(
      internal.id,
      internal.name,
      internal.description,
      convertFormat(internal.format),
      internal.schemaString,
      internal.partitionColumns.toList.asJava,
      java.util.Optional.ofNullable(internal.createdTime.getOrElse(null).asInstanceOf[Long]),
      internal.schema)
  }

  def convertFormat(internal: Format): FormatJ = {
    new FormatJ(internal.provider, internal.options.asJava)
  }
}
