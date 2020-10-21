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

import java.sql.{Date, Timestamp}
import java.util
import java.util.TimeZone

import collection.JavaConverters._
import scala.collection.compat.Factory
import scala.reflect.ClassTag

import com.github.mjakubowski84.parquet4s._
import io.delta.alpine.data.{RowRecord => RowParquetRecordJ}
import io.delta.alpine.internal.exception.DeltaErrors
import io.delta.alpine.types._

private[internal] case class RowParquetRecordImpl(
    private val record: RowParquetRecord,
    private val schema: StructType,
    private val timeZone: TimeZone) extends RowParquetRecordJ {

  private val codecConf = ValueCodecConfiguration(timeZone)

  ///////////////////////////////////////////////////////////////////////////
  // Public API Methods
  ///////////////////////////////////////////////////////////////////////////

  override def getInt(fieldName: String): Int = getAs[Int](fieldName)

  override def getLong(fieldName: String): Long = getAs[Long](fieldName)

  override def getByte(fieldName: String): Byte = getAs[Byte](fieldName)

  override def getShort(fieldName: String): Short = getAs[Short](fieldName)

  override def getBoolean(fieldName: String): Boolean = getAs[Boolean](fieldName)

  override def getFloat(fieldName: String): Float = getAs[Float](fieldName)

  override def getDouble(fieldName: String): Double = getAs[Double](fieldName)

  override def getString(fieldName: String): String = getAs[String](fieldName)

  override def getBinary(fieldName: String): Array[Byte] = getAs[Array[Byte]](fieldName)

  override def getBigDecimal(fieldName: String): java.math.BigDecimal =
    getAs[java.math.BigDecimal](fieldName)

  override def getTimestamp(fieldName: String): Timestamp = getAs[Timestamp](fieldName)

  override def getDate(fieldName: String): Date = getAs[Date](fieldName)

  override def getRecord(fieldName: String): RowParquetRecordJ = getAs[RowParquetRecordJ](fieldName)

  override def getList[T](fieldName: String): util.List[T] = getAs[util.List[T]](fieldName)

  override def getMap[K, V](fieldName: String): util.Map[K, V] = getAs[util.Map[K, V]](fieldName)

  ///////////////////////////////////////////////////////////////////////////
  // Decoding Helper Methods
  ///////////////////////////////////////////////////////////////////////////

  private def getAs[T](fieldName: String): T = {
    val schemaField = schema.get(fieldName);
    val parquetVal = record.get(fieldName)

    if (null == schemaField) throw DeltaErrors.noFieldFoundInSchema(fieldName, schema)

    if (parquetVal == NullValue && !schemaField.isNullable) {
      throw DeltaErrors.nullValueFoundForNonNullSchemaField(fieldName, schema)
    }

    decode(schemaField.getDataType, parquetVal).asInstanceOf[T]
  }

  private def decode(elemType: DataType, parquetVal: Value): Any = {
    val elemTypeName = elemType.getTypeName
    if (primitiveDecodeMap.contains(elemTypeName)) {
      return primitiveDecodeMap(elemTypeName).decode(parquetVal, codecConf)
    }

    (elemType, parquetVal) match {
      case (x: ArrayType, y: ListParquetRecord) => decodeList(x.getElementType, y)
      case (x: MapType, y: MapParquetRecord) => decodeMap(x.getKeyType, x.getValueType, y)
      case (x: StructType, y: RowParquetRecord) => RowParquetRecordImpl(y, x, timeZone)
      case _ => throw new RuntimeException("Unknown non-primitive decode type") // TODO DeltaErrors?
    }
  }

  private def decodeList(elemType: DataType, list: ListParquetRecord): Any = {
    val elemTypeName = elemType.getTypeName

    if (seqDecodeMap.contains(elemTypeName)) {
      // List of primitives
      return seqDecodeMap(elemTypeName).decode(list, codecConf).asJava
    }

    elemType match {
      case x: ArrayType =>
        // List of lists
        list.map { case y: ListParquetRecord =>
          y.map(z => decode(x.getElementType, z)).asJava
        }.asJava
      case x: MapType =>
        // List of maps
        list.map { case y: MapParquetRecord => decodeMap(x.getKeyType, x.getValueType, y) }.asJava
      case x: StructType =>
        // List of records
        list.map { case y: RowParquetRecord => RowParquetRecordImpl(y, x, timeZone) }.asJava
      case _ => throw new RuntimeException("Unknown non-primitive list decode type")
    }
  }

  private def decodeMap(
      keyType: DataType,
      valueType: DataType,
      parquetVal: MapParquetRecord): java.util.Map[Any, Any] = {
    parquetVal.map { case (keyParquetVal, valParquetVal) =>
      decode(keyType, keyParquetVal) -> decode(valueType, valParquetVal)
    }.toMap.asJava
  }

  ///////////////////////////////////////////////////////////////////////////
  // Useful Custom Decoders and type -> decoder Maps
  ///////////////////////////////////////////////////////////////////////////

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
          case _ => throw new RuntimeException("Unknown decimal decode type")
        }
      }

      override def encodeNonNull(
        data: java.math.BigDecimal,
        configuration: ValueCodecConfiguration): Value = {
        throw new UnsupportedOperationException("Shouldn't be encoding in the reader")
      }
    }

  private def customSeqCodec[T](elementCodec: ValueCodec[T])(implicit
    classTag: ClassTag[T],
    factory: Factory[T, Seq[T]]): ValueCodec[Seq[T]] = new OptionalValueCodec[Seq[T]] {
    override def decodeNonNull(
      value: Value,
      configuration: ValueCodecConfiguration): Seq[T] = {
      value match {
        case listRecord: ListParquetRecord =>
          listRecord.map(elementCodec.decode(_, codecConf))
        case binaryValue: BinaryValue if classTag.runtimeClass == classOf[Byte] =>
          binaryValue.value.getBytes.asInstanceOf[Seq[T]]
        case _ => throw new RuntimeException("Unknown list decode type")
      }
    }

    override def encodeNonNull(
      data: Seq[T],
      configuration: ValueCodecConfiguration): Value = {
      throw new UnsupportedOperationException("Shouldn't be encoding in the reader")
    }
  }

  private val primitiveDecodeMap = Map(
    new IntegerType().getTypeName -> ValueCodec.intCodec,
    new LongType().getTypeName -> ValueCodec.longCodec,
    new ByteType().getTypeName -> ValueCodec.byteCodec,
    new ShortType().getTypeName -> ValueCodec.shortCodec,
    new BooleanType().getTypeName -> ValueCodec.booleanCodec,
    new FloatType().getTypeName -> ValueCodec.floatCodec,
    new DoubleType().getTypeName -> ValueCodec.doubleCodec,
    new StringType().getTypeName -> ValueCodec.stringCodec,
    new BinaryType().getTypeName -> ValueCodec.arrayCodec[Byte, Array],
    new DecimalType(1, 1).getTypeName -> customDecimalCodec,
    new TimestampType().getTypeName -> ValueCodec.sqlTimestampCodec,
    new DateType().getTypeName -> ValueCodec.sqlDateCodec
  )

  private val seqDecodeMap = Map(
    new IntegerType().getTypeName -> customSeqCodec[Int](ValueCodec.intCodec),
    new LongType().getTypeName -> customSeqCodec[Long](ValueCodec.longCodec),
    new ByteType().getTypeName -> customSeqCodec[Byte](ValueCodec.byteCodec),
    new ShortType().getTypeName -> customSeqCodec[Short](ValueCodec.shortCodec),
    new BooleanType().getTypeName -> customSeqCodec[Boolean](ValueCodec.booleanCodec),
    new FloatType().getTypeName -> customSeqCodec[Float](ValueCodec.floatCodec),
    new DoubleType().getTypeName -> customSeqCodec[Double](ValueCodec.doubleCodec),
    new StringType().getTypeName -> customSeqCodec[String](ValueCodec.stringCodec),
    new BinaryType().getTypeName -> customSeqCodec[Array[Byte]](ValueCodec.arrayCodec[Byte, Array]),
    new DecimalType(1, 1).getTypeName ->
      customSeqCodec[java.math.BigDecimal](customDecimalCodec),
    new TimestampType().getTypeName -> customSeqCodec[Timestamp](ValueCodec.sqlTimestampCodec),
    new DateType().getTypeName -> customSeqCodec[Date](ValueCodec.sqlDateCodec)
  )
}
