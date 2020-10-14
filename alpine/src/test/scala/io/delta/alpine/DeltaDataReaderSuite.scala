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
      new Date(i))

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
        val i = row.getInt("as_int")

        assert(row.getLong("as_long") == i.longValue)
        assert(row.getByte("as_byte") == i.byteValue)
        assert(row.getShort("as_short") == i.shortValue)
        assert(row.getBoolean("as_boolean") == (i % 2 == 0))
        assert(row.getFloat("as_float") == i.floatValue)
        assert(row.getDouble("as_float") == i.doubleValue)
        assert(row.getString("as_string") == i.toString)
//        assert(row.getTimestamp("as_timestamp") == new Timestamp(i)) // TODO TimeZone fix
//        assert(row.getDate("as_date") == new Date(i)) // TODO TimeZone fix
      }
    }
  }

  // TODO test bad cast
  // TODO test the iterator
  // TODO test complicated structures, arrays, maps?
}
