/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.flink.sink.internal;

import java.util.Arrays;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

public class TestSchemaConverter {

    @Test
    public void testConvertFlinkSchemaToDeltaSchema() {
        // GIVEN
        RowType flinkRowType = new RowType(
            Arrays.asList(
                new RowType.RowField("f1", new FloatType()),
                new RowType.RowField("f2", new IntType()),
                new RowType.RowField("f3", new VarCharType()),
                new RowType.RowField("f4", new DoubleType()),
                new RowType.RowField("f5", new VarCharType()),
                new RowType.RowField("f6", new BooleanType()),
                new RowType.RowField("f7", new TinyIntType()),
                new RowType.RowField("f8", new SmallIntType()),
                new RowType.RowField("f9", new BigIntType()),
                new RowType.RowField("f10", new BinaryType()),
                new RowType.RowField("f11", new VarBinaryType()),
                new RowType.RowField("f12", new TimestampType()),
                new RowType.RowField("f13", new DateType()),
                new RowType.RowField("f14", new CharType()),
                new RowType.RowField("f15", new DecimalType()),
                new RowType.RowField("f16", new DecimalType(2)),
                new RowType.RowField("f17", new DecimalType(2, 2)),
                new RowType.RowField("f18", new DecimalType(38, 2)),
                new RowType.RowField("f19", new DecimalType(10, 1))
            ));

        // WHEN
        StructType deltaStructType = SchemaConverter.toDeltaDataType(flinkRowType);

        // THEN
        StructType expectedDeltaStructType = new StructType(
            new StructField[]{
                new StructField("f1", new io.delta.standalone.types.FloatType()),
                new StructField("f2", new io.delta.standalone.types.IntegerType()),
                new StructField("f3", new io.delta.standalone.types.StringType()),
                new StructField("f4", new io.delta.standalone.types.DoubleType()),
                new StructField("f5", new io.delta.standalone.types.StringType()),
                new StructField("f6", new io.delta.standalone.types.BooleanType()),
                new StructField("f7", new io.delta.standalone.types.ByteType()),
                new StructField("f8", new io.delta.standalone.types.ShortType()),
                new StructField("f9", new io.delta.standalone.types.LongType()),
                new StructField("f10", new io.delta.standalone.types.BinaryType()),
                new StructField("f11", new io.delta.standalone.types.BinaryType()),
                new StructField("f12", new io.delta.standalone.types.TimestampType()),
                new StructField("f13", new io.delta.standalone.types.DateType()),
                new StructField("f14", new io.delta.standalone.types.StringType()),
                new StructField("f15", new io.delta.standalone.types.DecimalType(10, 0)),
                new StructField("f16", new io.delta.standalone.types.DecimalType(2, 0)),
                new StructField("f17", new io.delta.standalone.types.DecimalType(2, 2)),
                new StructField("f18", new io.delta.standalone.types.DecimalType(38, 2)),
                new StructField("f19", new io.delta.standalone.types.DecimalType(10, 1))
            });

        assertEquals(expectedDeltaStructType, deltaStructType);
    }
}
