package org.apache.flink.connector.delta.sink;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


public class DeltaSinkIT {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new VarBinaryType(VarBinaryType.MAX_LENGTH),
                    new BooleanType(),
                    new TinyIntType(),
                    new SmallIntType(),
                    new IntType(),
                    new BigIntType(),
                    new FloatType(),
                    new DoubleType(),
                    new TimestampType(9),
                    new DecimalType(5, 0),
                    new DecimalType(15, 0),
                    new DecimalType(20, 0));

    protected static final int NUM_SOURCES = 4;

    protected static final int NUM_SINKS = 1;

    protected static final int NUM_RECORDS = 1000;

    protected static final int NUM_BUCKETS = 4;

    protected static final double FAILOVER_RATIO = 0.4;

    String PATH = "/data/projects/connectors-pk/flink-connector/src/test/resources/test/4";

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Parameterized.Parameter
    public boolean triggerFailover;

    @Parameterized.Parameters(name = "triggerFailover = {0}")
    public static Collection<Object[]> params() {
        return Arrays.asList(new Object[]{false}, new Object[]{true});
    }

    @Test
    public void testFileSink() throws Exception {
        //String path = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(new File(PATH));


        JobGraph jobGraph = createJobGraph();

        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "18081-19000");
        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();

        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }

    }

    protected JobGraph createJobGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        env.configure(config, getClass().getClassLoader());
        env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        ParquetWriterFactory<RowData> factory = ParquetRowDataBuilder.createWriterFactory(ROW_TYPE, conf, true);
        ParquetRowDataBuilder.FlinkParquetBuilder writerBuilder = new ParquetRowDataBuilder.FlinkParquetBuilder(ROW_TYPE, conf, true);

//        DeltaParquetWriterFactory<RowData> deltaWriterFactory = new DeltaParquetWriterFactory<>(writerBuilder, conf);

        DeltaSink<RowData> deltaSink = DeltaSink
                .forBulkFormat(new Path(PATH), conf, factory)
                .build();

        FileSink<RowData> fileSink = FileSink
                .forBulkFormat(new Path(PATH), factory)
                .build();

        env.fromCollection(getTestRows())
                .setParallelism(1)
                .sinkTo(deltaSink)
                .setParallelism(NUM_SINKS);

        StreamGraph streamGraph = env.getStreamGraph();
        return streamGraph.getJobGraph();
    }



    protected List<RowData> getTestRows() {
        List<RowData> rows = new ArrayList<>(NUM_RECORDS);
        for (int i = 0; i < NUM_RECORDS; i++) {
            Integer v = i;
            rows.add(
                    CONVERTER.toInternal(
                            Row.of(
                                    String.valueOf(v),
                                    String.valueOf(v).getBytes(StandardCharsets.UTF_8),
                                    v % 2 == 0,
                                    v.byteValue(),
                                    v.shortValue(),
                                    v,
                                    v.longValue(),
                                    v.floatValue(),
                                    v.doubleValue(),
                                    toDateTime(v),
                                    BigDecimal.valueOf(v),
                                    BigDecimal.valueOf(v),
                                    BigDecimal.valueOf(v))
                    )
            );
        }
        return rows;
    }


    private static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER =
            DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(ROW_TYPE)
            );

    private LocalDateTime toDateTime(Integer v) {
        v = (v > 0 ? v : -v) % 1000;
        return LocalDateTime.now().plusNanos(v).plusSeconds(v);
    }

}
