/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

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

package main.scala.types

/**
 * Contains case classes for the following types:
 * - ArrayType
 * - MapType
 * - StructField
 * - BinaryType
 * - BooleanType
 * - ByteType
 * - CalendarIntervalType
 * - DateType
 * - DecimalType
 * - DoubleType
 * - FloatType
 * - IntegerType
 * - LongType
 * - NullType
 * - ShortType
 * - StringType
 * - TimestampType
 */

/**
 * The data type for collections of multiple values.
 * Internally these are represented as columns that contain a ``scala.collection.Seq``.
 *
 * An [[ArrayType]] object comprises two fields, `elementType: [[DataType]]` and
 * `containsNull: Boolean`. The field of `elementType` is used to specify the type of
 * array elements. The field of `containsNull` is used to specify if the array has `null` values.
 *
 * @param elementType The data type of values.
 * @param containsNull Indicates if values have `null` values
 */
case class ArrayType(elementType: DataType, containsNull: Boolean) extends DataType {

  // TODO: private[sql]
  def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    builder.append(
      s"$prefix-- element: ${elementType.typeName} (containsNull = $containsNull)\n")
    DataType.buildFormattedString(elementType, s"$prefix    |", builder)
  }

  /**
   * The default size of a value of the ArrayType is the default size of the element type.
   * We assume that there is only 1 element on average in an array. See SPARK-18853.
   */
  override def defaultSize: Int = 1 * elementType.defaultSize
}

/**
 * The data type for Maps. Keys in a map are not allowed to have `null` values.
 *
 * @param keyType The data type of map keys.
 * @param valueType The data type of map values.
 * @param valueContainsNull Indicates if map values have `null` values.
 */
case class MapType(
  keyType: DataType,
  valueType: DataType,
  valueContainsNull: Boolean) extends DataType {

  // TODO: private[sql]
  def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    builder.append(s"$prefix-- key: ${keyType.typeName}\n")
    DataType.buildFormattedString(keyType, s"$prefix    |", builder)
    builder.append(s"$prefix-- value: ${valueType.typeName} " +
      s"(valueContainsNull = $valueContainsNull)\n")
    DataType.buildFormattedString(valueType, s"$prefix    |", builder)
  }

  /**
   * The default size of a value of the MapType is
   * (the default size of the key type + the default size of the value type).
   * We assume that there is only 1 element on average in a map. See SPARK-18853.
   */
  override def defaultSize: Int = 1 * (keyType.defaultSize + valueType.defaultSize)
}

/**
 * A field inside a StructType.
 * @param name The name of this field.
 * @param dataType The data type of this field.
 * @param nullable Indicates if values of this field can be `null` values.
 * @param metadata The metadata of this field. The metadata should be preserved during
 *                 transformation if the content of the column is not modified, e.g, in selection.
 */
case class StructField(
  name: String,
  dataType: DataType,
  nullable: Boolean = true,
  metadata: Metadata = Metadata.empty) {

  // TODO: private[sql]
  def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    builder.append(s"$prefix-- $name: ${dataType.typeName} (nullable = $nullable)\n")
    DataType.buildFormattedString(dataType, s"$prefix    |", builder)
  }
}

/**
 * The data type representing `Array[Byte]` values.
 */
class BinaryType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "BinaryType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 100
}

case object BinaryType extends BinaryType

/**
 * The data type representing `Boolean` values.
 */
class BooleanType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "BooleanType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 1
}

case object BooleanType extends BooleanType

/**
 * The data type representing `Byte` values.
 */
class ByteType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "ByteType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 1
  override def simpleString: String = "tinyint"
}

case object ByteType extends ByteType

/**
 * The data type representing calendar time intervals. The calendar time interval is stored
 * internally in two components: number of months the number of microseconds.
 */
class CalendarIntervalType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "CalendarIntervalType$" in byte
  // code. Defined with a private constructor so the companion object is the only possible
  // instantiation.
  override def defaultSize: Int = 16
}

case object CalendarIntervalType extends CalendarIntervalType

/**
 * A date type, supporting "0001-01-01" through "9999-12-31".
 * Internally, this is represented as the number of days from 1970-01-01.
 */
class DateType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "DateType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 4
}

case object DateType extends DateType

/**
 * The data type representing `java.math.BigDecimal` values.
 * A Decimal that must have fixed precision (the maximum number of digits) and scale (the number
 * of digits on right side of dot).
 *
 * The precision can be up to 38, scale can also be up to 38 (less or equal to precision).
 *
 * The default precision and scale is (10, 0).
 */
case class DecimalType(precision: Int, scale: Int) extends DataType {
  override def defaultSize: Int = if (precision <= DecimalType.MAX_LONG_DIGITS) 8 else 16
}

object DecimalType {
  val MAX_LONG_DIGITS = 18
  val USER_DEFAULT: DecimalType = DecimalType(10, 0)
}

/**
 * The data type representing `Double` values.
 */
class DoubleType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "DoubleType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 8
}

case object DoubleType extends DoubleType

/**
 * The data type representing `Float` values.
 */
class FloatType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "FloatType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 4
}

case object FloatType extends FloatType

/**
 * The data type representing `Int` values.
 */
class IntegerType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "IntegerType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 4
  override def simpleString: String = "int"
}

case object IntegerType extends IntegerType

/**
 * The data type representing `Long` values.
 */
class LongType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "LongType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 8
  override def simpleString: String = "bigint"
}

case object LongType extends LongType

/**
 * The data type representing `NULL` values.
 */
class NullType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "NullType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 1
}

case object NullType extends NullType

/**
 * The data type representing `Short` values.
 */
class ShortType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "ShortType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 2
  override def simpleString: String = "smallint"
}

case object ShortType extends ShortType

/**
 * The data type representing `String` values.
 */
class StringType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "StringType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 20
}

case object StringType extends StringType

/**
 * The data type representing `java.sql.Timestamp` values.
 */
class TimestampType private() extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "TimestampType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 8
}

case object TimestampType extends TimestampType
