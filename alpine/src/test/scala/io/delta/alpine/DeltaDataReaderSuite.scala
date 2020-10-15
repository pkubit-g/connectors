package io.delta.alpine

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class DeltaDataReaderSuite extends QueryTest with SharedSparkSession {
  private def createRow(i: Int): Row = {
    Row(i, i.longValue, i.byteValue, i.shortValue, i % 2 == 0, i.floatValue, i.doubleValue,
      i.toString,
      new Timestamp(i),
      new Date(i),
      Array(i, i, i),
      Array(Array(i, i), Array(i, i))
    )
  }

  private val schema = new StructType()
    .add("as_int", IntegerType)
    .add("as_long", LongType)
    .add("as_byte", ByteType)
    .add("as_short", ShortType)
    .add("as_boolean", BooleanType)
    .add("as_float", FloatType)
    .add("as_double", DoubleType)
    .add("as_string", StringType)
    .add("as_timestamp", TimestampType)
    .add("as_date", DateType)
    .add("as_array", ArrayType(IntegerType))
    .add("as_array_of_arrays", ArrayType(ArrayType(IntegerType)))

  test("correctly read") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val data = (0 until 10).map(createRow(_))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.format("delta").save(tblLoc)
      df.printSchema()
      df.show()

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
        assert(row.getAs[Array[Int]]("as_array") sameElements Array(i, i, i))
        assert(row.getAs[Array[Array[Int]]]("as_array_of_arrays").deep ==
          Array(Array(i, i), Array(i, i)).deep)
//      assert(row.getTimestamp("as_timestamp") == new Timestamp(i)) // TODO TimeZone fix
//      assert(row.getDate("as_date") == new Date(i)) // TODO TimeZone fix
      }
    }
  }

  // TODO test bad cast
  // TODO test the iterator
  // TODO test complicated structures, arrays, maps?
  // TODO test nullable
}
