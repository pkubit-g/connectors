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

import scala.reflect.ClassTag

import com.github.mjakubowski84.parquet4s._
import io.delta.alpine.data.{RowParquetRecord => RowParquetRecordJ}
import io.delta.alpine.types._

// TODO pass in the timezone by parsing it from Hadoop Conf
private[internal] case class RowParquetRecordImpl(
    private val record: RowParquetRecord,

  // TODO performance optimization
  // private val schemaFieldTypes: scala.collection.immutable.Map[String, DataType]
    private val schema: StructType,
    private val timeZone: TimeZone = TimeZone.getTimeZone(ZoneOffset.UTC)
)
  extends RowParquetRecordJ {
  private val codecConf = ValueCodecConfiguration(timeZone)

//  override def getInt(fieldName: String): Int = record.get[Int](fieldName, codecConf)
//
//  override def getLong(fieldName: String): Long = record.get[Long](fieldName, codecConf)
//
//  override def getByte(fieldName: String): Byte = record.get[Byte](fieldName, codecConf)
//
//  override def getShort(fieldName: String): Short = record.get[Short](fieldName, codecConf)
//
//  override def getBoolean(fieldName: String): Boolean = record.get[Boolean](fieldName, codecConf)
//
//  override def getFloat(fieldName: String): Float = record.get[Float](fieldName, codecConf)
//
//  override def getDouble(fieldName: String): Double = record.get[Double](fieldName, codecConf)
//
//  override def getString(fieldName: String): String = record.get[String](fieldName, codecConf)
//
//  override def getTimestamp(fieldName: String): Timestamp =
//    record.get[Timestamp](fieldName, codecConf)
//
//  override def getDate(fieldName: String): Date = record.get[Date](fieldName, codecConf)
//
//  override def getByteArray(fieldName: String): Array[Byte] =
//    record.get[Array[Byte]](fieldName, codecConf)
//
//  override def getRecord(fieldName: String): RowParquetRecordJ = {
//    val nestedRecord = record.get[RowParquetRecord](fieldName, codecConf)
//    RowParquetRecordImpl(nestedRecord, timeZone)
//  }
//
//  override def getArray[T](fieldName: String): Array[T] = {
//    record.get(fieldName).asInstanceOf[ListParquetRecord].map()
//  }

  override def getAs[T](fieldName: String): T = {
    val schemaField = schema.getDataTypeForField(fieldName);
    val parquetVal = record.get(fieldName)

    if (null == schemaField || (parquetVal == NullValue && !schemaField.isNullable)) {
      throw new Exception("todo")
    }

    decode(schemaField.getDataType, parquetVal).asInstanceOf[T]
  }

  private def decode(elemType: DataType, parquetVal: Value): Any = {
    // TODO null check?
    (elemType, parquetVal) match {
        // TODO null type ?
      case (_: IntegerType, _) =>
        ValueCodec.intCodec.decode(parquetVal, codecConf)
      case (_: LongType, _) =>
        ValueCodec.longCodec.decode(parquetVal, codecConf)
      case (_: ByteType, _) =>
        ValueCodec.byteCodec.decode(parquetVal, codecConf)
      case (_: ShortType, _) =>
        ValueCodec.shortCodec.decode(parquetVal, codecConf)
      case (_: BooleanType, _) =>
        ValueCodec.booleanCodec.decode(parquetVal, codecConf)
      case (_: FloatType, _) =>
        ValueCodec.floatCodec.decode(parquetVal, codecConf)
      case (_: DoubleType, _) =>
        ValueCodec.doubleCodec.decode(parquetVal, codecConf)
      case (_: StringType, _) =>
        ValueCodec.stringCodec.decode(parquetVal, codecConf)
      case (_: DecimalType, _: IntValue) =>
        new java.math.BigDecimal(ValueCodec.intCodec.decode(parquetVal, codecConf))
      case (_: DecimalType, _) =>
        // TODO test this case
        ValueCodec.decimalCodec.decode(parquetVal, codecConf)

      case (x: ArrayType, y: ListParquetRecord) =>
        decodeArray(x.getElementType, y)
      case (x: StructType, y: RowParquetRecord) =>
        RowParquetRecordImpl(y, x, timeZone)
      case (x: MapType, y: MapParquetRecord) =>
        decodeMap(x.getKeyType, x.getValueType, y)
    }
  }

  private def decodeMap(
      keyType: DataType,
      valueType: DataType,
      parquetVal: MapParquetRecord): Any = {
    (keyType, valueType, parquetVal) match {
      case (_: IntegerType, _: IntegerType, _) =>
        ValueCodec.mapCodec[Int, Int].decode(parquetVal, codecConf)
    }
  }

  private def decodeArray(arrayElemType: DataType, parquetVal: ListParquetRecord): Any = {
    arrayElemType match {
      case _: IntegerType =>
        ValueCodec.arrayCodec[Int, Array].decode(parquetVal, codecConf)
      case _: LongType =>
        ValueCodec.arrayCodec[Long, Array].decode(parquetVal, codecConf)
      case _: ByteType =>
        ValueCodec.arrayCodec[Byte, Array].decode(parquetVal, codecConf)
      case _: ShortType =>
        ValueCodec.arrayCodec[Short, Array].decode(parquetVal, codecConf)
      case _: BooleanType =>
        ValueCodec.arrayCodec[Boolean, Array].decode(parquetVal, codecConf)
      case _: FloatType =>
        ValueCodec.arrayCodec[Float, Array].decode(parquetVal, codecConf)
      case _: DoubleType =>
        ValueCodec.arrayCodec[Double, Array].decode(parquetVal, codecConf)
      case _: StringType =>
        ValueCodec.arrayCodec[String, Array].decode(parquetVal, codecConf)
      case _: DecimalType =>
        val elemIter = parquetVal.iterator
        if (elemIter.hasNext && elemIter.next().isInstanceOf[IntValue]) {
          ValueCodec.arrayCodec[Int, Array].decode(parquetVal, codecConf)
            .map(new java.math.BigDecimal(_))
        } else {
          ValueCodec.arrayCodec[BigDecimal, Array].decode(parquetVal, codecConf)
        }
      case x: ArrayType =>
        parquetVal.map { y =>
          y.asInstanceOf[ListParquetRecord].map(z => decode(x.getElementType, z))
        }.toArray
      // TODO MAP
      // TODO record
    }
  }
}
