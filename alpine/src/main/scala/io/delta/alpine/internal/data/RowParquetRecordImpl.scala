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

import scala.collection.compat.Factory
import scala.reflect.ClassTag

import com.github.mjakubowski84.parquet4s._
import io.delta.alpine.data.{RowParquetRecord => RowParquetRecordJ}
import io.delta.alpine.types._

// TODO pass in the timezone by parsing it from Hadoop Conf
private[internal] case class RowParquetRecordImpl(
    private val record: RowParquetRecord,
    private val schema: StructType,
    private val timeZone: TimeZone = TimeZone.getTimeZone(ZoneOffset.UTC))
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

  private val customDecimalCodec: ValueCodec[java.math.BigDecimal] =
      new OptionalValueCodec[java.math.BigDecimal] {
    override def decodeNonNull(
        value: Value,
        configuration: ValueCodecConfiguration): java.math.BigDecimal = {
      value match {
        // parquet4s.ValueCodec.decimalCodec doesn't match on IntValue
        case IntValue(int) => new java.math.BigDecimal(int)
        case DoubleValue(double) => BigDecimal.decimal(double).bigDecimal
        case FloatValue(float) => BigDecimal.decimal(float).bigDecimal
        case BinaryValue(binary) => Decimals.decimalFromBinary(binary).bigDecimal
      }
    }

    override def encodeNonNull(
        data: java.math.BigDecimal,
        configuration: ValueCodecConfiguration): Value = {
      throw new UnsupportedOperationException("TODO")
      }
  }

  private def customArrayDecimalCodec(implicit
      classTag: ClassTag[java.math.BigDecimal],
      factory: Factory[java.math.BigDecimal, Array[java.math.BigDecimal]]
  ): ValueCodec[Array[java.math.BigDecimal]] = new OptionalValueCodec[Array[java.math.BigDecimal]] {
    override def decodeNonNull(
        value: Value,
        configuration: ValueCodecConfiguration): Array[java.math.BigDecimal] =
      value match {
        case listRecord: ListParquetRecord =>
          listRecord.map(customDecimalCodec.decode(_, codecConf)).toArray
        case binaryValue: BinaryValue if classTag.runtimeClass == classOf[Byte] =>
          binaryValue.value.getBytes.asInstanceOf[Array[java.math.BigDecimal]]
      }

    override def encodeNonNull(
        data: Array[java.math.BigDecimal],
        configuration: ValueCodecConfiguration): Value = {
      throw new UnsupportedOperationException("TODO")
    }
  }

  val primitiveDecodeMap = Map(
    new IntegerType().getTypeName -> ValueCodec.intCodec,
    new LongType().getTypeName -> ValueCodec.longCodec,
    new ByteType().getTypeName -> ValueCodec.byteCodec,
    new ShortType().getTypeName -> ValueCodec.shortCodec,
    new BooleanType().getTypeName -> ValueCodec.booleanCodec,
    new FloatType().getTypeName -> ValueCodec.floatCodec,
    new DoubleType().getTypeName -> ValueCodec.doubleCodec,
    new StringType().getTypeName -> ValueCodec.stringCodec,
    new DecimalType(1, 1).getTypeName -> customDecimalCodec
  )

  val arrayDecodeMap = Map(
    new IntegerType().getTypeName -> ValueCodec.arrayCodec[Int, Array],
    new LongType().getTypeName -> ValueCodec.arrayCodec[Long, Array],
    new ByteType().getTypeName -> ValueCodec.arrayCodec[Byte, Array],
    new ShortType().getTypeName -> ValueCodec.arrayCodec[Short, Array],
    new BooleanType().getTypeName -> ValueCodec.arrayCodec[Boolean, Array],
    new FloatType().getTypeName -> ValueCodec.arrayCodec[Float, Array],
    new DoubleType().getTypeName -> ValueCodec.arrayCodec[Double, Array],
    new StringType().getTypeName -> ValueCodec.arrayCodec[String, Array],
    new DecimalType(1, 1).getTypeName -> customArrayDecimalCodec
  )

  override def getAs[T](fieldName: String): T = {
    val schemaField = schema.getDataTypeForField(fieldName);
    val parquetVal = record.get(fieldName)

    if (null == schemaField || (parquetVal == NullValue && !schemaField.isNullable)) {
      throw new Exception("todo")
    }

    decode(schemaField.getDataType, parquetVal).asInstanceOf[T]
  }

  private def decode(elemType: DataType, parquetVal: Value): Any = {
    val elemTypeName = elemType.getTypeName
    if (primitiveDecodeMap.contains(elemTypeName)) {
      return primitiveDecodeMap(elemTypeName).decode(parquetVal, codecConf)
    }

    (elemType, parquetVal) match {
      case (x: ArrayType, y: ListParquetRecord) => decodeArray(x.getElementType, y)
      case (x: MapType, y: MapParquetRecord) => decodeMap(x.getKeyType, x.getValueType, y)
      case (x: StructType, y: RowParquetRecord) => RowParquetRecordImpl(y, x, timeZone)
    }
  }

  private def decodeArray(elemType: DataType, list: ListParquetRecord): Any = {
    val elemTypeName = elemType.getTypeName
    if (arrayDecodeMap.contains(elemTypeName)) {
      // Array of primitive
      return arrayDecodeMap(elemTypeName).decode(list, codecConf)
    }

    elemType match {
      // Array of Array
      case x: ArrayType =>
        list.map { case y: ListParquetRecord =>
          val temp = y.map(z => decode(x.getElementType, z))
          if (x.getElementType.isInstanceOf[ArrayType]) {
            // Array of Array of Array
            temp.toArray
          } else {
            temp
          }
        }.toArray
      case x: StructType =>
        list.map { case y: RowParquetRecord => RowParquetRecordImpl(y, x, timeZone) }.toArray
    }
  }

  private def decodeMap(
      keyType: DataType,
      valueType: DataType,
      parquetVal: MapParquetRecord): Any = {
    parquetVal.map { case (keyParquetVal, valParquetVal) =>
      decode(keyType, keyParquetVal) -> decode(valueType, valParquetVal)
    }.toMap
  }
}
