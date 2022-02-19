package io.delta.flink.sink;

import java.util.LinkedHashMap;
import java.util.List;

import io.delta.flink.sink.internal.DeltaBucketAssignerInternal;
import io.delta.flink.sink.internal.DeltaPartitionComputer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class DeltaBucketAssigner<T> extends DeltaBucketAssignerInternal<T> {

    private DeltaBucketAssigner(DeltaPartitionComputer<T> partitionComputer) {
        super(partitionComputer);
    }

    /**
     * Creates instance of partition computer for {@link RowData}
     *
     * @param rowType       logical schema of the records in the stream/table
     * @param partitionKeys list of partition column names in the order they should be applied
     *                      when creating a destination path
     */
    public static DeltaBucketAssigner<RowData> forRowData(RowType rowType,
                                                   List<String> partitionKeys) {
        return forRowData(rowType, partitionKeys, new LinkedHashMap<>());
    }

    /**
     * Creates instance of partition computer for {@link RowData} with a provided set of static
     * partitions.
     * <p>
     * Provided static partitions' values will have preference over the values for those fields in
     * the incoming events. e.g. if we set bucket assigner as below"
     * <pre>
     *     RowType testRowType = new RowType(Arrays.asList(
     *         new RowType.RowField("partition_col1", new VarCharType()),
     *         new RowType.RowField("partition_col2", new VarCharType()),
     *         new RowType.RowField("col2", new VarCharType())
     *     ));
     *     List&lt;String&gt; partitionCols = Arrays.asList("partition_col1", "partition_col2");
     *     LinkedHashMap&lt;String, String&gt; staticPartitionValues =
     *         new LinkedHashMap&lt;String, String&gt;() {{ put("partition_col2", "static_val"); }};
     *
     *     DeltaBucketAssigner&lt;RowData&gt; bucketAssigner =
     *     DeltaBucketAssigner.forRowData(testRowType, partitionCols, staticPartitionValues);
     * </pre>
     * then data will be stored under path:
     * `"partition_col1=&lt;partition_col1_from_event&gt;/partition_col2=static_val/"` with omitting
     * `partition_col2` values from the events.
     *
     * @param rowType             logical schema of the records in the stream/table
     * @param partitionKeys       list of partition column names in the order they should be
     *                            applied when creating a destination path
     * @param staticPartitionSpec static values for partitions that should set explicitly
     *                            instead of being derived from the content of the records
     */
    public static DeltaBucketAssigner<RowData> forRowData(
        RowType rowType,
        List<String> partitionKeys,
        LinkedHashMap<String, String> staticPartitionSpec) {
        DeltaPartitionComputer<RowData> partitionComputer =
            new DeltaPartitionComputer.DeltaRowDataPartitionComputer(
                rowType, partitionKeys, staticPartitionSpec);
        return new DeltaBucketAssigner<>(partitionComputer);
    }
}
