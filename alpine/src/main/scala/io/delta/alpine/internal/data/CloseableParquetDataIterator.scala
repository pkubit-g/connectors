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

package io.delta.alpine.internal.data

import java.util.TimeZone

import com.github.mjakubowski84.parquet4s._
import com.github.mjakubowski84.parquet4s.ParquetReader.Options
import io.delta.alpine.data.{CloseableIterator, RowRecord => RowParquetRecordJ}
import io.delta.alpine.types.StructType

private[internal] case class CloseableParquetDataIterator(
    dataFilePaths: Seq[String],
    schema: StructType,
    timeZoneId: String) extends CloseableIterator[RowParquetRecordJ] {
  private val readTimeZone = if (null == timeZoneId) TimeZone.getDefault
    else TimeZone.getTimeZone(timeZoneId)

  private val dataFilePathsIter = dataFilePaths.iterator
  private var parquetRows = if (dataFilePathsIter.hasNext) readNextFile else null
  private var parquetRowsIter = if (null != parquetRows) parquetRows.iterator else null

  override def hasNext: Boolean = {
    // Base case when initialized to null
    if (null == parquetRows || null == parquetRowsIter) {
      close()
      return false
    }

    // More rows in current file
    if (parquetRowsIter.hasNext) return true

    // No more rows in current file and no more files
    if (!dataFilePathsIter.hasNext) {
      close()
      return false
    }

    // No more rows in this file, but there is a next file
    parquetRows.close()
    parquetRows = readNextFile
    parquetRowsIter = parquetRows.iterator
    parquetRowsIter.hasNext
  }

  override def next(): RowParquetRecordJ = {
    if (!hasNext) throw new NoSuchElementException
    val row = parquetRowsIter.next()
    RowParquetRecordImpl(row, schema, readTimeZone)
  }

  override def close(): Unit = {
    if (null != parquetRows) {
      parquetRows.close()
      parquetRows = null
      parquetRowsIter = null
    }
  }

  private def readNextFile: ParquetIterable[RowParquetRecord] = {
    ParquetReader.read[RowParquetRecord](
      s"${dataFilePathsIter.next()}", Options(readTimeZone))
  }
}
