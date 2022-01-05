package io.delta.flink.example.sink;

import io.delta.flink.sink.DeltaSink;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public abstract class DeltaSinkExampleBase implements DeltaSinkLocalJobRunner {

    static int PRINT_PAD_LENGTH = 4;

    public static final RowType ROW_TYPE = new RowType(Arrays.asList(
            new RowType.RowField("f1", new VarCharType(VarCharType.MAX_LENGTH)),
            new RowType.RowField("f2", new VarCharType(VarCharType.MAX_LENGTH)),
            new RowType.RowField("f3", new IntType())
    ));

    void run(String tablePath) throws IOException, InterruptedException {
        System.out.println("Will use table path: " + tablePath);
        File tableDir = new File(tablePath);
        if (tableDir.list().length > 0) {
            FileUtils.cleanDirectory(tableDir);
        }
        StreamExecutionEnvironment env = getFlinkStreamExecutionEnvironment(tablePath);
        runFlinkJobInBackground(env);
        printDeltaTableRows(tablePath);
    }

    DeltaSink<RowData> getDeltaSink(String tablePath) {
        throw new RuntimeException("Not implemented");
    }

    private StreamExecutionEnvironment getFlinkStreamExecutionEnvironment(String tablePath) {
        DeltaSink<RowData> deltaSink = getDeltaSink(tablePath);
        StreamExecutionEnvironment env = getStreamExecutionEnvironment();
        env.addSource(new DeltaSinkExampleSourceFunction())
                .setParallelism(2)
                .sinkTo(deltaSink)
                .setParallelism(3);
        return env;
    }

    public static void printDeltaTableRows(String tablePath) throws InterruptedException {
        DeltaLog deltaLog =
                DeltaLog.forTable(new org.apache.hadoop.conf.Configuration(), tablePath);

        for (int i = 0; i < 30; i++) {
            deltaLog.update();
            Snapshot snapshot = deltaLog.snapshot();

            System.out.println("===== current snapshot =====");
            System.out.println("snapshot version: " + snapshot.getVersion());
            System.out.println("number of total data files: " + snapshot.getAllFiles().size());

            CloseableIterator<RowRecord> iter = snapshot.open();
            System.out.println("\ntable rows:");
            System.out.println(StringUtils.rightPad("f1", PRINT_PAD_LENGTH) + "| " +
                    StringUtils.rightPad("f2", PRINT_PAD_LENGTH) + " | " +
                    StringUtils.rightPad("f3", PRINT_PAD_LENGTH));
            System.out.println(String.join("", Collections.nCopies(4 * PRINT_PAD_LENGTH, "-")));

            RowRecord row = null;
            int numRows = 0;
            while (iter.hasNext()) {
                row = iter.next();
                numRows++;

                String f1 = row.isNullAt("f1") ? null : row.getString("f1");
                String f2 = row.isNullAt("f2") ? null : row.getString("f2");
                Integer f3 = row.isNullAt("f3") ? null : row.getInt("f3");

                System.out.println(StringUtils.rightPad(f1, PRINT_PAD_LENGTH) + "| " +
                        StringUtils.rightPad(f2, PRINT_PAD_LENGTH) + " | " +
                        StringUtils.rightPad(String.valueOf(f3), PRINT_PAD_LENGTH));
            }
            System.out.println("\nnumber rows: " + numRows);
            if (row != null) {
                System.out.println("data schema:");
                System.out.println(row.getSchema().getTreeString());
                System.out.println("partition cols:");
                System.out.println(snapshot.getMetadata().getPartitionColumns());
            }
            System.out.println("\n");
            Thread.sleep(5000);
        }
    }
}
