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

package io.delta.flink.sink;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;

/**
 * Custom implementation of {@link BucketAssigner} class required to provide behaviour on how
 * to map particular events to buckets (aka partitions).
 * <p>
 * This implementation can be perceived as a utility class for complying to the DeltaLake's
 * partitioning style (that follows Apache Hive's partitioning style by providing the partitioning
 * column's and its values as FS directories paths, e.g. "/some_path/table_1/date=2020-01-01")
 * It's still possible for users to roll out their own version of {@link BucketAssigner}
 * and pass it to the {@link DeltaSinkBuilder} during creation of the sink.
 *
 * @param <T> The type of input elements.
 */
public class DeltaTablePartitionAssigner<T> implements BucketAssigner<T, String> {

    private static final long serialVersionUID = -6033643154550226022L;

    private final DeltaPartitionComputer<T> partitionComputer;

    public DeltaTablePartitionAssigner(DeltaPartitionComputer<T> partitionComputer) {
        this.partitionComputer = partitionComputer;
    }

    @Override
    public String getBucketId(T element, BucketAssigner.Context context) {
        LinkedHashMap<String, String> partitionValues =
            this.partitionComputer.generatePartitionValues(element, context);
        return PartitionPathUtils.generatePartitionPath(partitionValues);
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }

    @Override
    public String toString() {
        return "DeltaTablePartitionAssigner";
    }

    public interface DeltaPartitionComputer<T> extends Serializable {

        /**
         * Compute partition values from record.
         * <p>
         * E.g.
         * If the table has two partitioning columns 'date' and 'country' then this method should
         * return linked hashmap like:
         * LinkedHashMap(
         * "date" -&gt; "2020-01-01",
         * "country" -&gt; "x"
         * )
         * <p>
         * for event that should be written to example path of:
         * '/some_path/table_1/date=2020-01-01/country=x'.
         *
         * @param element input record.
         * @param context {@link BucketAssigner.Context} that can be used during partition's
         *                assignment
         * @return partition values.
         */
        LinkedHashMap<String, String> generatePartitionValues(
            T element, BucketAssigner.Context context);
    }

    /**
     * Implementation of {@link DeltaPartitionComputer} for stream which elements are instances of
     * {@link RowData}.
     * <p>
     * This partition computer resolves partition values by extracting them from element's fields
     * by provided partitions' names. This behaviour can be overridden by providing static values
     * for partitions' fields.
     */
    public static class DeltaRowDataPartitionComputer implements DeltaPartitionComputer<RowData> {

        private final LinkedHashMap<String, String> staticPartitionSpec;
        RowType rowType;
        List<String> partitionKeys;

        /**
         * Creates instance of partition computer for {@link RowData}
         *
         * @param rowType       logical schema of the records in the stream/table
         * @param partitionKeys list of partition column names in the order they should be applied
         *                      when creating a destination path
         */
        public DeltaRowDataPartitionComputer(RowType rowType,
                                             List<String> partitionKeys) {
            this.rowType = rowType;
            this.partitionKeys = partitionKeys;
            this.staticPartitionSpec = new LinkedHashMap<>();
        }

        /**
         * Creates instance of partition computer for {@link RowData}
         *
         * @param rowType             logical schema of the records in the stream/table
         * @param partitionKeys       list of partition column names in the order they should be
         *                            applied when creating a destination path
         * @param staticPartitionSpec static values for partitions that should not be derived from
         *                            the content of the records
         */
        public DeltaRowDataPartitionComputer(RowType rowType,
                                             List<String> partitionKeys,
                                             LinkedHashMap<String, String> staticPartitionSpec) {
            this.rowType = rowType;
            this.partitionKeys = partitionKeys;
            this.staticPartitionSpec = staticPartitionSpec;
        }

        @Override
        public LinkedHashMap<String, String> generatePartitionValues(RowData element,
                                                                     Context context) {
            LinkedHashMap<String, String> partitionValues = new LinkedHashMap<>();

            for (String partitionKey : partitionKeys) {
                int keyIndex = rowType.getFieldIndex(partitionKey);
                LogicalType keyType = rowType.getTypeAt(keyIndex);

                if (staticPartitionSpec.containsKey(partitionKey)) {
                    partitionValues.put(partitionKey, staticPartitionSpec.get(partitionKey));
                } else if (keyType.getTypeRoot() == LogicalTypeRoot.VARCHAR) {
                    partitionValues.put(partitionKey, element.getString(keyIndex).toString());
                } else if (keyType.getTypeRoot() == LogicalTypeRoot.INTEGER) {
                    partitionValues.put(partitionKey, String.valueOf(element.getInt(keyIndex)));
                } else if (keyType.getTypeRoot() == LogicalTypeRoot.BIGINT) {
                    partitionValues.put(partitionKey, String.valueOf(element.getLong(keyIndex)));
                } else if (keyType.getTypeRoot() == LogicalTypeRoot.SMALLINT) {
                    partitionValues.put(partitionKey, String.valueOf(element.getShort(keyIndex)));
                } else if (keyType.getTypeRoot() == LogicalTypeRoot.TINYINT) {
                    partitionValues.put(partitionKey, String.valueOf(element.getByte(keyIndex)));
                } else {
                    throw new RuntimeException("Type not supported " + keyType.getTypeRoot());
                }
            }
            return partitionValues;
        }
    }
}
