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

package main.scala

import java.io.Closeable
import java.util.NoSuchElementException

import com.github.mjakubowski84.parquet4s._
import main.scala.types._

trait ClosableIterator[T] extends Iterator[T] with Closeable {
  override def hasNext: Boolean

  override def next(): T

  override def close(): Unit
}

class CloseableParquetDataIterator(
    val dataFilePaths: Set[String],
    val dataPath: String,
    val dataSchema: StructType) extends ClosableIterator[Array[(String, Any)]] {

  private val dataFilePathsIter = dataFilePaths.iterator
  private var parquetRows = if (dataFilePathsIter.hasNext) readNextFile else null
  private var parquetRowsIter = if (null != parquetRows) parquetRows.iterator else null


  override def hasNext: Boolean = {
    if (null == parquetRows) return false // base case when dataFilePathsIter initially empty
    if (parquetRowsIter.hasNext) return true // still more rows in the current file
    if (!dataFilePathsIter.hasNext) { // no more files
      close()
      return false
    }

    // No more rows in this file, but there is a next file
    parquetRows.close()
    parquetRows = readNextFile
    parquetRowsIter = parquetRows.iterator
    parquetRowsIter.hasNext
  }

  override def next(): Array[(String, Any)] = {
    if (!hasNext) throw new NoSuchElementException
    parseRow(parquetRowsIter.next())
  }

  // TODO should we set fields to null here, too?
  override def close(): Unit = {
    if (null != parquetRows) {
      parquetRows.close()
    }
  }

  private def readNextFile: ParquetIterable[RowParquetRecord] = ParquetReader
    .read[RowParquetRecord](s"$dataPath/${dataFilePathsIter.next()}")

  private def parseRow(row: RowParquetRecord): Array[(String, Any)] = {
    dataSchema.map { case StructField(fieldName, dataType, nullable, _) =>
      val parquetValue = row.get(fieldName)

      if (parquetValue == NullValue && !nullable) {
        throw new Exception("todo")
      }

      val scalaVal = (parquetValue, dataType) match {
        case (x: IntValue, _: IntegerType) => x.value
        case (x: BinaryValue, _: StringType) => x.value.toStringUsingUTF8
        case (x: BinaryValue, _: BinaryType) => x.value.getBytes
        case _ => null
      }

      (fieldName, scalaVal)
    }
  }.toArray
}
