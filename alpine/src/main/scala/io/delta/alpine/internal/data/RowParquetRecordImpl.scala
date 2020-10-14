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

import scala.reflect.runtime.universe._

import java.sql.{Date, Timestamp}
import java.time.ZoneOffset
import java.util.TimeZone

import com.github.mjakubowski84.parquet4s._
import io.delta.alpine.data.{RowParquetRecord => RowParquetRecordJ}

// TODO pass in the timezone by parsing it from Hadoop Conf
private[internal] case class RowParquetRecordImpl(
    private val record: RowParquetRecord,
    private val timeZone: TimeZone = TimeZone.getTimeZone(ZoneOffset.UTC))
  extends RowParquetRecordJ {

  private val codecConf = ValueCodecConfiguration(timeZone)

  /*
  override def getInt(fieldName: String): Int = record.get[Int](fieldName, codecConf)

  override def getLong(fieldName: String): Long = record.get[Long](fieldName, codecConf)

  override def getByte(fieldName: String): Byte = record.get[Byte](fieldName, codecConf)

  override def getShort(fieldName: String): Short = record.get[Short](fieldName, codecConf)

  override def getBoolean(fieldName: String): Boolean = record.get[Boolean](fieldName, codecConf)

  override def getFloat(fieldName: String): Float = record.get[Float](fieldName, codecConf)

  override def getDouble(fieldName: String): Double = record.get[Double](fieldName, codecConf)

  override def getString(fieldName: String): String = record.get[String](fieldName, codecConf)

  override def getTimestamp(fieldName: String): Timestamp =
    record.get[Timestamp](fieldName, codecConf)

  override def getDate(fieldName: String): Date = record.get[Date](fieldName, codecConf)

//  override def getByteArray(fieldName: String): Array[Byte] =
//    record.get[Array[Byte]](fieldName, codecConf)

  override def getRecord(fieldName: String): RowParquetRecordJ = {
    val nestedRecord = record.get[RowParquetRecord](fieldName, codecConf)
    RowParquetRecordImpl(nestedRecord, timeZone)
  }

  override def getArray[T](fieldName: String): Array[T] = record.get[Array[T]](fieldName, codecConf)
 **/

  override def get[T](fieldName: String): T = {
    typeOf[T] match {
      case t if t =:= typeOf[String] => "".asInstanceOf[T]
    }
  }
}
