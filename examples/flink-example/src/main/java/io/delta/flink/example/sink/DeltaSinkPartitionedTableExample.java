package io.delta.flink.example.sink;

import java.util.Arrays;
import java.util.List;

import io.delta.flink.sink.DeltaPartitionComputer;
import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.DeltaSinkBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;

/**
 * Demonstrates how the Flink Delta Sink can be used to write data to a partitioned Delta table.
 * <p>
 * If you run this example then application will spawn example local Flink job generating data to
 * the underlying Delta table under directory of "src/main/resources/example_table". The job will be
 * run in a daemon thread while in the main app's thread there will Delta Standalone application
 * reading and printing all the data to the std out.
 */
public class DeltaSinkPartitionedTableExample extends DeltaSinkExampleBase {

    static String TABLE_PATH = resolveExampleTableAbsolutePath("example_partitioned_table");

    public static void main(String[] args) throws Exception {
        new DeltaSinkPartitionedTableExample().run(TABLE_PATH);
    }

    @Override
    DeltaSink<RowData> getDeltaSink(String tablePath) {
        List<String> partitionCols = Arrays.asList("f1");
        DeltaPartitionComputer<RowData> partitionComputer =
            DeltaPartitionComputer.forRowData(ROW_TYPE, partitionCols);

        DeltaSinkBuilder<RowData> deltaSinkBuilder = DeltaSink.forRowData(
            new Path(TABLE_PATH), new Configuration(), ROW_TYPE);
        deltaSinkBuilder.withPartitionComputer(partitionComputer);
        return deltaSinkBuilder.build();
    }
}
