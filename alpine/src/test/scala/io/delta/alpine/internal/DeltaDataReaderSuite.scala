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

package io.delta.alpine.internal

import java.math.{BigDecimal => JBigDecimal}
import java.sql.Timestamp
import java.util.{TimeZone, List => JList, Map => JMap}
import java.util.Arrays.{asList => asJList}

import scala.collection.JavaConverters._

import io.delta.alpine.data.{RowRecord => JRowRecord}
import io.delta.alpine.DeltaLog
import io.delta.alpine.internal.sources.AlpineHadoopConf
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class DeltaDataReaderSuite extends QueryTest with SharedSparkSession {

  private def writeDataPrintInfo(tblLoc: String, data: Seq[Row], schema: StructType): Unit = {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.write.format("delta").save(tblLoc)
    df.printSchema()
    df.show(false)
  }

  test("read - primitives") {
    withTempDir { dir =>
      def createRow(i: Int): Row = {
        Row(i, i.longValue, i.toByte, i.shortValue, i % 2 == 0, i.floatValue, i.doubleValue,
          i.toString, Array[Byte](i.toByte, i.toByte), new JBigDecimal(i))
      }

      val schema = new StructType()
        .add("as_int", IntegerType)
        .add("as_long", LongType)
        .add("as_byte", ByteType)
        .add("as_short", ShortType)
        .add("as_boolean", BooleanType)
        .add("as_float", FloatType)
        .add("as_double", DoubleType)
        .add("as_string", StringType)
        .add("as_binary", BinaryType)
        .add("as_big_decimal", DecimalType(1, 0))

      val tblLoc = dir.getCanonicalPath
      val data = (0 until 10).map(createRow)
      writeDataPrintInfo(tblLoc, data, schema)

      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)
      val recordIter = alpineLog.snapshot().open()

      while (recordIter.hasNext) {
        val row = recordIter.next()
        val i = row.getInt("as_int")
        assert(row.getLong("as_long") == i.longValue)
        assert(row.getByte("as_byte") == i.toByte)
        assert(row.getShort("as_short") == i.shortValue)
        assert(row.getBoolean("as_boolean") == (i % 2 == 0))
        assert(row.getFloat("as_float") == i.floatValue)
        assert(row.getDouble("as_double") == i.doubleValue)
        assert(row.getString("as_string") == i.toString)
        assert(row.getBinary("as_binary") sameElements Array[Byte](i.toByte, i.toByte))
        assert(row.getBigDecimal("as_big_decimal") == new JBigDecimal(i))
      }
    }
  }

  test("read - date types") {
    Seq("UTC", "Iceland", "PST", "America/Los_Angeles", "Etc/GMT+9", "Asia/Beirut",
      "JST").foreach { timeZoneId =>
      withTempDir { dir =>
        val timeZone = TimeZone.getTimeZone(timeZoneId)
        TimeZone.setDefault(timeZone)

        val timestamp = Timestamp.valueOf("2020-01-01 08:09:10")
        val date = java.sql.Date.valueOf("2020-01-01")

        val data = Row(timestamp, date) :: Nil
        val schema = new StructType()
          .add("timestamp", TimestampType)
          .add("date", DateType)

        val tblLoc = dir.getCanonicalPath
        writeDataPrintInfo(tblLoc, data, schema)

        val hadoopConf = new Configuration()
        hadoopConf.set(AlpineHadoopConf.PARQUET_DATA_TIME_ZONE_ID, timeZoneId)

        val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)
        val recordIter = alpineLog.snapshot().open()

        val row = recordIter.next()

        assert(row.getTimestamp("timestamp").equals(timestamp))
        assert(row.getDate("date").equals(date))

        recordIter.close()
      }
    }
  }

  test("read - array of primitives") {
    withTempDir { dir =>
      def createRow(i: Int): Row = {
        Row(Array(i), Array(i.longValue), Array(i.toByte), Array(i.shortValue),
          Array(i % 2 == 0), Array(i.floatValue), Array(i.doubleValue), Array(i.toString),
          Array(Array(i.toByte, i.toByte)),
          Array(new JBigDecimal(i))
        )
      }

      val schema = new StructType()
        .add("as_array_int", ArrayType(IntegerType))
        .add("as_array_long", ArrayType(LongType))
        .add("as_array_byte", ArrayType(ByteType))
        .add("as_array_short", ArrayType(ShortType))
        .add("as_array_boolean", ArrayType(BooleanType))
        .add("as_array_float", ArrayType(FloatType))
        .add("as_array_double", ArrayType(DoubleType))
        .add("as_array_string", ArrayType(StringType))
        .add("as_array_binary", ArrayType(BinaryType))
        .add("as_array_big_decimal", ArrayType(DecimalType(1, 0)))

      val tblLoc = dir.getCanonicalPath
      val data = (0 until 10).map(createRow)
      writeDataPrintInfo(tblLoc, data, schema)

      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)
      val recordIter = alpineLog.snapshot().open()

      while (recordIter.hasNext) {
        val row = recordIter.next()
        val list = row.getList[Int]("as_array_int")
        val i = list.get(0)

        assert(row.getList[Long]("as_array_long") == asJList(i.toLong))
        assert(row.getList[Byte]("as_array_byte") == asJList(i.toByte))
        assert(row.getList[Short]("as_array_short") == asJList(i.shortValue))
        assert(row.getList[Boolean]("as_array_boolean") == asJList(i % 2 == 0))
        assert(row.getList[Float]("as_array_float") == asJList(i.floatValue))
        assert(row.getList[Double]("as_array_double") == asJList(i.doubleValue))
        assert(row.getList[String]("as_array_string") == asJList(i.toString))
        assert(row.getList[Array[Byte]]("as_array_binary").get(0) sameElements
          Array(i.toByte, i.toByte))
        assert(row.getList[JBigDecimal]("as_array_big_decimal") == asJList(new JBigDecimal(i)))
      }
    }
  }

  test("read - array of complex objects") {
    withTempDir { dir =>
      def createRow(i: Int): Row = {
        Row(
          i,
          Array(Array(Array(i, i, i), Array(i, i, i)), Array(Array(i, i, i), Array(i, i, i))),
          Array(
            Array(Array(Array(i, i, i), Array(i, i, i)), Array(Array(i, i, i), Array(i, i, i))),
            Array(Array(Array(i, i, i), Array(i, i, i)), Array(Array(i, i, i), Array(i, i, i)))
          ),
          Array(
            Map[String, Long](i.toString -> i.toLong),
            Map[String, Long](i.toString -> i.toLong)
          ),
          Array(Row(i), Row(i), Row(i))
        )
      }

      val schema = new StructType()
        .add("i", IntegerType)
        .add("3d_int_list", ArrayType(ArrayType(ArrayType(IntegerType))))
        .add("4d_int_list", ArrayType(ArrayType(ArrayType(ArrayType(IntegerType)))))
        .add("list_of_maps", ArrayType(MapType(StringType, LongType)))
        .add("list_of_records", ArrayType(new StructType().add("val", IntegerType)))

      val tblLoc = dir.getCanonicalPath
      val data = (0 until 10).map(createRow)
      writeDataPrintInfo(tblLoc, data, schema)

      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)
      val recordIter = alpineLog.snapshot().open()

      while (recordIter.hasNext) {
        val row = recordIter.next()
        val i = row.getInt("i")
        assert(
          row.getList[JList[JList[Int]]]("3d_int_list") ==
          asJList(
            asJList(asJList(i, i, i), asJList(i, i, i)),
            asJList(asJList(i, i, i), asJList(i, i, i))
          )
        )

        assert(
          row.getList[JList[JList[JList[Int]]]]("4d_int_list") ==
            asJList(
              asJList(
                asJList(asJList(i, i, i), asJList(i, i, i)),
                asJList(asJList(i, i, i), asJList(i, i, i))
              ),
              asJList(
                asJList(asJList(i, i, i), asJList(i, i, i)),
                asJList(asJList(i, i, i), asJList(i, i, i))
              )
            )
        )

        assert(
          row.getList[JMap[String, Long]]("list_of_maps") ==
          asJList(
            Map[String, Long](i.toString -> i.toLong).asJava,
            Map[String, Long](i.toString -> i.toLong).asJava
          )
        )

        val recordList = row.getList[JRowRecord]("list_of_records")
        recordList.forEach(nestedRow => assert(nestedRow.getInt("val") == i))
      }
    }
  }

  test("read - map") {
    withTempDir { dir =>
      def createRow(i: Int): Row = {
        Row(
          i,
          Map(i -> i),
          Map(i.toLong -> i.toByte),
          Map(i.toShort -> (i % 2 == 0)),
          Map(i.toFloat -> i.toDouble),
          Map(i.toString -> new JBigDecimal(i)),
          Map(i -> Array(Row(i), Row(i), Row(i)))
        )
      }

      val schema = new StructType()
        .add("i", IntegerType)
        .add("a", MapType(IntegerType, IntegerType))
        .add("b", MapType(LongType, ByteType))
        .add("c", MapType(ShortType, BooleanType))
        .add("d", MapType(FloatType, DoubleType))
        .add("e", MapType(StringType, DecimalType(1, 0)))
        .add("f", MapType(IntegerType, ArrayType(new StructType().add("val", IntegerType))))

      val tblLoc = dir.getCanonicalPath
      val data = (0 until 10).map(createRow)
      writeDataPrintInfo(tblLoc, data, schema)

      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)
      val recordIter = alpineLog.snapshot().open()

      while (recordIter.hasNext) {
        val row = recordIter.next()
        val i = row.getInt("i")
        assert(row.getMap[Int, Int]("a").equals(Map(i -> i).asJava))
        assert(row.getMap[Long, Byte]("b").equals(Map(i.toLong -> i.toByte).asJava))
        assert(row.getMap[Short, Boolean]("c").equals(Map(i.toShort -> (i % 2 == 0)).asJava))
        assert(row.getMap[Float, Double]("d").equals(Map(i.toFloat -> i.toDouble).asJava))
        assert(
          row.getMap[String, JBigDecimal]("e").equals(Map(i.toString -> new JBigDecimal(i)).asJava)
        )

        val mapOfRecordList = row.getMap[Int, java.util.List[JRowRecord]]("f")
        val recordList = mapOfRecordList.get(i)
        recordList.forEach(nestedRow => assert(nestedRow.getInt("val") == i))
      }
    }
  }

  test("read - nested struct") {
    withTempDir { dir =>
      def createRow(i: Int): Row = Row(Row(i.toString, i.toString, Row(i, i.toLong)), i)

      val schema = new StructType()
        .add("a", new StructType()
          .add("aa", StringType)
          .add("ab", StringType)
          .add("ac", new StructType()
            .add("aca", IntegerType)
            .add("acb", LongType)
          )
        )
        .add("b", IntegerType)

      val tblLoc = dir.getCanonicalPath
      val data = (0 until 10).map(createRow)
      writeDataPrintInfo(tblLoc, data, schema)

      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)
      val recordIter = alpineLog.snapshot().open()

      while (recordIter.hasNext) {
        val row = recordIter.next()
        val i = row.getInt("b")
        val nestedStruct = row.getRecord("a")
        assert(nestedStruct.getString("aa") == i.toString)
        assert(nestedStruct.getString("ab") == i.toString)

        val nestedNestedStruct = nestedStruct.getRecord("ac")
        assert(nestedNestedStruct.getInt("aca") == i)
        assert(nestedNestedStruct.getLong("acb") == i.toLong)
      }
    }
  }

  test("read - nullable field, invalid schema column key") {
    withTempDir { dir =>
      val data = Row(Seq(null, null, null)) :: Nil
      val schema = new StructType()
        .add("array_can_contain_null", ArrayType(StringType, containsNull = true))

      val tblLoc = dir.getCanonicalPath
      writeDataPrintInfo(tblLoc, data, schema)

      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)
      val recordIter = alpineLog.snapshot().open()

      val row = recordIter.next()
      row.getList[String]("array_can_contain_null").forEach(elem => assert(elem == null))

      val e = intercept[IllegalArgumentException] {
        row.getInt("foo_key_does_not_exist")
      }
      assert(e.getMessage.contains("No matching schema column for field"))

      recordIter.close()
    }
  }

  test("test absolute path") {

  }
}
