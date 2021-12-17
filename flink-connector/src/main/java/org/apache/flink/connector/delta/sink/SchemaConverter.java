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

package org.apache.flink.connector.delta.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.delta.standalone.types.*;

/**
 * This is a utility class to convert from Flink's specific {@link RowType} into
 * DeltaLake's specific {@link StructType} which is used for schema-matching comparisons
 * during {@link io.delta.standalone.DeltaLog} commits.
 */
@Internal
public class SchemaConverter {

    /**
     * Main method for converting from {@link RowType} into {@link StructType}
     *
     * @param rowType Flink's logical type of stream's events
     * @return DeltaLake's specific type of stream's events
     */
    public static StructType toDeltaDataType(RowType rowType) {
        StructField[] fields = rowType.getFields()
                .stream()
                .map(rowField -> {
                    DataType rowFieldType = toDeltaDataType(rowField.getType());
                    return new StructField(
                            rowField.getName(),
                            rowFieldType,
                            rowField.getType().isNullable());
                })
                .toArray(StructField[]::new);

        return new StructType(fields);
    }

    /**
     * Method containing the actual mapping between Flink's and DeltaLake's types.
     *
     * @param flinkType Flink's logical type
     * @return DeltaLake's data type
     */
    public static DataType toDeltaDataType(LogicalType flinkType) {
        switch (flinkType.getTypeRoot()) {
            case BIGINT:
                return new LongType();
            case BINARY:
            case VARBINARY:
                return new BinaryType();
            case BOOLEAN:
                return new BooleanType();
            case DATE:
                return new DateType();
            case DECIMAL:
                org.apache.flink.table.types.logical.DecimalType decimalType =
                        (org.apache.flink.table.types.logical.DecimalType) flinkType;
                return new DecimalType(decimalType.getPrecision(), decimalType.getScale());
            case DOUBLE:
                return new DoubleType();
            case FLOAT:
                return new FloatType();
            case INTEGER:
                return new IntegerType();
            case NULL:
                return new NullType();
            case SMALLINT:
                return new ShortType();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new TimestampType();
            case TINYINT:
                return new ByteType();
            case CHAR:
            case VARCHAR:
                return new StringType();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type: " + flinkType);
        }
    }
}
