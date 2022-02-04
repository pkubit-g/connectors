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
package io.delta.flink.table;

import java.util.LinkedHashMap;
import java.util.Map;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.DeltaSinkBuilder;
import io.delta.flink.sink.DeltaTablePartitionAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

/**
 * Sink of a dynamic Flink table to a Delta lake table.
 *
 * <p>
 * It utilizes new Flink Sink API (available for Flink &gt;= 1.12) and interfaces (available for
 * Flink &gt;= 1.13) provided for interoperability between this new Sink API and Table API. It also
 * supports static partitioning.
 *
 * <p>
 * For regular batch scenarios, the sink can solely accept insert-only rows and write out bounded
 * streams.
 *
 * <p>
 * For regular streaming scenarios, the sink can solely accept insert-only rows and can write out
 * unbounded streams.
 */
public class DeltaDynamicTableSink implements DynamicTableSink, SupportsPartitioning {

    Path basePath;
    Configuration conf;
    RowType rowType;
    boolean mergeSchema;
    CatalogTable catalogTable;
    private LinkedHashMap<String, String> staticPartitionSpec = new LinkedHashMap<>();

    /**
     * Constructor for creating sink of Flink dynamic table to Delta table.
     *
     * @param basePath     full Delta table path
     * @param conf         Hadoop's configuration
     * @param rowType      Flink's logical type with the structure of the events in the
     *                     stream
     * @param mergeSchema  whether we should try to update table's schema with stream's
     *                     schema in case those will not match
     * @param catalogTable represents the unresolved metadata of derived by Flink framework
     *                     from table's DDL
     */
    public DeltaDynamicTableSink(
        final Path basePath,
        Configuration conf,
        final RowType rowType,
        boolean mergeSchema,
        CatalogTable catalogTable
    ) {
        this.basePath = basePath;
        this.rowType = rowType;
        this.conf = conf;
        this.catalogTable = catalogTable;
        this.mergeSchema = mergeSchema;
    }

    /**
     * Returns the set of changes that the sink accepts during runtime.
     *
     * @param requestedMode expected set of changes by the current plan
     * @return {@link ChangelogMode} only allowing for inserts to the Delta table
     */
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    /**
     * Utility method for transitions between Flink's DataStream and Table API.
     *
     * @param context Context for creating runtime implementation via a
     *                {@link SinkRuntimeProvider}.
     * @return provider representing {@link DeltaSink} implementation for writing the data to a
     * Delta table.
     */
    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DeltaSinkBuilder<RowData> deltaSinkBuilder = DeltaSink.forRowData(
            this.basePath, this.conf, this.rowType
        ).withMergeSchema(mergeSchema);

        if (catalogTable.isPartitioned()) {
            DeltaTablePartitionAssigner.DeltaRowDataPartitionComputer partitionComputer =
                new DeltaTablePartitionAssigner.DeltaRowDataPartitionComputer(
                    rowType, catalogTable.getPartitionKeys(), staticPartitionSpec);
            DeltaTablePartitionAssigner<RowData> partitionAssigner =
                new DeltaTablePartitionAssigner<>(partitionComputer);
            deltaSinkBuilder.withBucketAssigner(partitionAssigner);
        }

        return SinkProvider.of(deltaSinkBuilder.build());
    }

    @Override
    public DynamicTableSink copy() {
        DeltaDynamicTableSink sink =
            new DeltaDynamicTableSink(basePath, conf, rowType, mergeSchema, catalogTable);
        sink.staticPartitionSpec = staticPartitionSpec;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "DeltaSink";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        // make it a LinkedHashMap to maintain partition column order
        staticPartitionSpec = new LinkedHashMap<>();
        for (String partitionCol : catalogTable.getPartitionKeys()) {
            if (partition.containsKey(partitionCol)) {
                staticPartitionSpec.put(partitionCol, partition.get(partitionCol));
            }
        }
    }
}
