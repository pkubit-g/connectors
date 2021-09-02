package io.delta.standalone.internal.data

import java.sql.{Date, Timestamp}

import io.delta.standalone.data.{RowRecord => RowRecordJ}
import io.delta.standalone.types.{BooleanType, IntegerType, StructType}

class PartitionRowRecord(
    partitionSchema: StructType,
    partitionValues: Map[String, String]) extends RowRecordJ {

// val partitionFieldToType =
//   partitionSchema.getFields.map { f => f.getName -> f.getDataType }.toMap

  override def getSchema: StructType = partitionSchema

  override def getLength: Int = partitionSchema.getFieldNames.length

  override def isNullAt(fieldName: String): Boolean = false

  override def getInt(fieldName: String): Int = {
//    require(partitionFieldToType.get(fieldName).exists(_.isInstanceOf[IntegerType]))
    partitionValues(fieldName).toInt
  }

  override def getLong(fieldName: String): Long = 0L

  override def getByte(fieldName: String): Byte = ???

  override def getShort(fieldName: String): Short = ???

  override def getBoolean(fieldName: String): Boolean = {
//    require(partitionFieldToType.get(fieldName).exists(_.isInstanceOf[BooleanType]))
    partitionValues(fieldName).toBoolean
  }

  override def getFloat(fieldName: String): Float = ???

  override def getDouble(fieldName: String): Double = ???

  override def getString(fieldName: String): String = ???

  override def getBinary(fieldName: String): Array[Byte] = ???

  override def getBigDecimal(fieldName: String): java.math.BigDecimal = ???

  override def getTimestamp(fieldName: String): Timestamp = ???

  override def getDate(fieldName: String): Date = ???

  override def getRecord(fieldName: String): RowRecordJ = ???

  override def getList[T](fieldName: String): java.util.List[T] = ???

  override def getMap[K, V](fieldName: String): java.util.Map[K, V] = ???
}
