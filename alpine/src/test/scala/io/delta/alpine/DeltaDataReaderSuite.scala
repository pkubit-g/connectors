package io.delta.alpine

import java.sql.{Date, Timestamp}

import io.delta.alpine.data.RowParquetRecord

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class DeltaDataReaderSuite extends QueryTest with SharedSparkSession {
  private def mapEquals[K, V](map1: Map[K, V], map2: Map[K, V]): Boolean = {
    (map1.toSet diff map2.toSet).isEmpty && (map2.toSet diff map1.toSet).isEmpty
  }

  private def writeDataPrintInfo(tblLoc: String, data: Seq[Row], schema: StructType): Unit = {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.write.format("delta").save(tblLoc)
    df.printSchema()
    df.show()
  }

  test("read - primitives") {
    withTempDir { dir =>
      def createRow(i: Int): Row = {
        Row(i, i.longValue, i.byteValue, i.shortValue, i % 2 == 0, i.floatValue, i.doubleValue,
          i.toString,
          new java.math.BigDecimal(i)
        )
      }

      // TODO char???
      val schema = new StructType()
        .add("as_int", IntegerType)
        .add("as_long", LongType)
        .add("as_byte", ByteType)
        .add("as_short", ShortType)
        .add("as_boolean", BooleanType)
        .add("as_float", FloatType)
        .add("as_double", DoubleType)
        .add("as_string", StringType)
        .add("as_big_decimal", DecimalType(1, 0))

      val tblLoc = dir.getCanonicalPath
      val data = (0 until 10).map(createRow)
      writeDataPrintInfo(tblLoc, data, schema)

      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)
      val recordIter = alpineLog.snapshot().open()

      while (recordIter.hasNext) {
        val row = recordIter.next()
        val i = row.getAs[Int]("as_int")
        assert(row.getAs[Long]("as_long") == i.longValue)
        assert(row.getAs[Byte]("as_byte") == i.byteValue())
        assert(row.getAs[Short]("as_short") == i.shortValue)
        assert(row.getAs[Boolean]("as_boolean") == (i % 2 == 0))
        assert(row.getAs[Float]("as_float") == i.floatValue)
        assert(row.getAs[Double]("as_double") == i.doubleValue)
        assert(row.getAs[String]("as_string") == i.toString)
        assert(row.getAs[java.math.BigDecimal]("as_big_decimal") == new java.math.BigDecimal(i))
      }
    }
  }

  test("read - array of primitives") {
    withTempDir { dir =>
      def createRow(i: Int): Row = {
        Row(Array(i), Array(i.longValue), Array(i.byteValue), Array(i.shortValue),
          Array(i % 2 == 0), Array(i.floatValue), Array(i.doubleValue), Array(i.toString),
          Array(new java.math.BigDecimal(i))
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
        .add("as_array_big_decimal", ArrayType(DecimalType(1, 0)))

      val tblLoc = dir.getCanonicalPath
      val data = (0 until 10).map(createRow)
      writeDataPrintInfo(tblLoc, data, schema)

      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)
      val recordIter = alpineLog.snapshot().open()

      while (recordIter.hasNext) {
        val row = recordIter.next()
        val arr = row.getAs[Array[Int]]("as_array_int")
        val i = arr(0)
        assert(row.getAs[Array[Long]]("as_array_long") sameElements Array(i))
        assert(row.getAs[Array[Byte]]("as_array_byte") sameElements Array(i.byteValue()))
        assert(row.getAs[Array[Short]]("as_array_short") sameElements Array(i.shortValue))
        assert(row.getAs[Array[Boolean]]("as_array_boolean") sameElements Array(i % 2 == 0))
        assert(row.getAs[Array[Float]]("as_array_float") sameElements Array(i.floatValue))
        assert(row.getAs[Array[Double]]("as_array_double") sameElements Array(i.doubleValue))
        assert(row.getAs[Array[String]]("as_array_string") sameElements Array(i.toString))
        assert(row.getAs[Array[java.math.BigDecimal]]("as_array_big_decimal")
          sameElements Array(new java.math.BigDecimal(i)))
      }
    }
  }

  test("read - array of complex objects") {
    withTempDir { dir =>
      def createRow(i: Int): Row = {
        Row(
          i,
          Array(Array(Array(i, i, i), Array(i, i, i)), Array(Array(i, i, i), Array(i, i, i))),
//          Array(
//            Array(Array(Array(i, i, i), Array(i, i, i)), Array(Array(i, i, i), Array(i, i, i))),
//            Array(Array(Array(i, i, i), Array(i, i, i)), Array(Array(i, i, i), Array(i, i, i)))
//          ),
          Array(
            Map[String, Long](i.toString -> i.toLong),
            Map[String, Long](i.toString -> i.toLong)
          ),
          Array(Row(i), Row(i), Row(i))
        )
      }

      val schema = new StructType()
        .add("i", IntegerType)
        .add("3d_int_array", ArrayType(ArrayType(ArrayType(IntegerType))))
//        .add("4d_int_array", ArrayType(ArrayType(ArrayType(ArrayType(IntegerType)))))
        .add("array_of_maps", ArrayType(MapType(StringType, LongType)))
        .add("array_of_records", ArrayType(new StructType().add("val", IntegerType)))

      val tblLoc = dir.getCanonicalPath
      val data = (0 until 10).map(createRow)
      writeDataPrintInfo(tblLoc, data, schema)

      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)
      val recordIter = alpineLog.snapshot().open()

      while (recordIter.hasNext) {
        val row = recordIter.next()
        val i = row.getAs[Int]("i")

        assert(row.getAs[Array[Array[Array[Int]]]]("3d_int_array").deep ==
          Array(Array(Array(i, i, i), Array(i, i, i)), Array(Array(i, i, i), Array(i, i, i))).deep)
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
          Map(i.toString -> new java.math.BigDecimal(i)),
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
        val i = row.getAs[Int]("i")

        assert(mapEquals(row.getAs[Map[Int, Int]]("a"), Map(i -> i)))
        assert(mapEquals(row.getAs[Map[Long, Byte]]("b"), Map(i.toLong -> i.toByte)))
        assert(mapEquals(row.getAs[Map[Short, Boolean]]("c"), Map(i.toShort -> (i % 2 == 0))))
        assert(mapEquals(row.getAs[Map[Float, Double]]("d"), Map(i.toFloat -> i.toDouble)))
        assert(mapEquals(
          row.getAs[Map[String, java.math.BigDecimal]]("e"),
          Map(i.toString -> new java.math.BigDecimal(i))
        ))

        val recordArrMap = row.getAs[Map[Int, Array[RowParquetRecord]]]("f")
        val recordArr = recordArrMap(i)
        assert(recordArr.forall(nestedRow => nestedRow.getAs[Int]("val") == i))
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
        val i = row.getAs[Int]("b")
        val nestedStruct = row.getAs[RowParquetRecord]("a")
        assert(nestedStruct.getAs[String]("aa") == i.toString)
        assert(nestedStruct.getAs[String]("ab") == i.toString)

        val nestedNestedStruct = nestedStruct.getAs[RowParquetRecord]("ac")
        assert(nestedNestedStruct.getAs[Int]("aca") == i)
        assert(nestedNestedStruct.getAs[Long]("acb") == i.toLong)
      }
    }
  }

  // TODO test bad cast
  // TODO test the iterator
  // TODO test complicated structures, arrays, maps?
  // TODO test nullable
}
