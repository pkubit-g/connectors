package io.delta.hive

import scala.collection.JavaConverters._

import main.scala.types._
import org.apache.hadoop.hive.metastore.api.MetaException
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport
import org.apache.hadoop.hive.serde2.typeinfo.{StructTypeInfo, TypeInfoFactory}

import org.apache.spark.SparkFunSuite

class DeltaHelperTest extends SparkFunSuite {

  test("DeltaHelper checkTableSchema correct") {
    // scalastyle:off
    val colNames = DataWritableReadSupport.getColumnNames("c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15")
    val colTypeInfos = DataWritableReadSupport.getColumnTypes("tinyint:binary:boolean:int:bigint:string:float:double:smallint:date:timestamp:decimal(38,18):array<string>:map<string,bigint>:struct<f1:string,f2:bigint>")
    // scalastyle:on
    val colDataTypes = Array(ByteType, BinaryType, BooleanType, IntegerType, LongType,
      StringType, FloatType, DoubleType, ShortType, DateType, TimestampType, DecimalType(38, 18),
      ArrayType(StringType, false), MapType(StringType, LongType, false),
      StructType(Array(StructField("f1", StringType), StructField("f2", LongType))))

    assert(colNames.size() == colTypeInfos.size() && colNames.size() == colDataTypes.size)

    val hiveSchema = TypeInfoFactory
      .getStructTypeInfo(colNames, colTypeInfos)
      .asInstanceOf[StructTypeInfo]

    val fields = colNames.asScala.zip(colDataTypes).map {
      case (name, dataType) => StructField(name, dataType)
    }

    val alpineSchema = StructType(fields)

    DeltaHelper.checkTableSchema(alpineSchema, hiveSchema)
  }

  test("DeltaHelper checkTableSchema incorrect throws") {
    val fields = Array(StructField("c1", IntegerType), StructField("c2", StringType))
    val alpineSchema = StructType(fields)

    def createHiveSchema(colNamesStr: String, colTypesStr: String): StructTypeInfo = {
      val colNames = DataWritableReadSupport.getColumnNames(colNamesStr)
      val colTypeInfos = DataWritableReadSupport.getColumnTypes(colTypesStr)

      TypeInfoFactory
        .getStructTypeInfo(colNames, colTypeInfos)
        .asInstanceOf[StructTypeInfo]
    }

    def assertSchemaException(hiveSchema: StructTypeInfo, exMsg: String): Unit = {
      val e = intercept[MetaException] {
        DeltaHelper.checkTableSchema(alpineSchema, hiveSchema)
      }
      assert(e.getMessage.contains("The Delta table schema is not the same as the Hive schema"))
      assert(e.getMessage.contains(exMsg))
    }

    // column number mismatch (additional field)
    val hiveSchema1 = createHiveSchema("c1,c2,c3", "int:string:boolean")
    assertSchemaException(hiveSchema1, "Specified schema has additional field(s): c3")

    // column name mismatch (mising field)
    val hiveSchema2 = createHiveSchema("c1,c3", "int:string")
    assertSchemaException(hiveSchema2, "Specified schema is missing field(s): c2")

    // column order mismatch
    val hiveSchema3 = createHiveSchema("c2,c1", "string:int")
    assertSchemaException(hiveSchema3, "Columns out of order")

    // column type mismatch
    val hiveSchema4 = createHiveSchema("c1,c2", "int:tinyint")
    assertSchemaException(hiveSchema4, "Specified type for c2 is different from existing schema")
  }
}
