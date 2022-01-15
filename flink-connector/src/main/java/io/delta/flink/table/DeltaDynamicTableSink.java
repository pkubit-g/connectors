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

public class DeltaDynamicTableSink implements DynamicTableSink, SupportsPartitioning {

    Path basePath;
    Configuration conf;
    RowType rowType;
    boolean shouldTryUpdateSchema;
    CatalogTable catalogTable;
    private LinkedHashMap<String, String> staticPartitionSpec = new LinkedHashMap<>();

    public DeltaDynamicTableSink(
        final Path basePath,
        Configuration conf,
        final RowType rowType,
        boolean shouldTryUpdateSchema,
        CatalogTable catalogTable
    ) {
        this.basePath = basePath;
        this.rowType = rowType;
        this.conf = conf;
        this.catalogTable = catalogTable;
        this.shouldTryUpdateSchema = shouldTryUpdateSchema;
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
     *                {@link DynamicTableSink.SinkRuntimeProvider}.
     * @return provider representing {@link DeltaSink} implementation for writing the data to a
     * Delta table.
     */
    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DeltaSinkBuilder<RowData> deltaSinkBuilder = DeltaSink.forRowData(
            this.basePath, this.conf, this.rowType
        ).withShouldTryUpdateSchema(shouldTryUpdateSchema);

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
            new DeltaDynamicTableSink(basePath, conf, rowType, shouldTryUpdateSchema, catalogTable);
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
