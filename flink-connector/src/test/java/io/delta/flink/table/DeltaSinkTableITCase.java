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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.sink.utils.TestParquetReader;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;

@RunWith(Parameterized.class)
public class DeltaSinkTableITCase {

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    public static final RowType TEST_ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("col1", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("col2", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("col3", new IntType())
    ));

    @Parameterized.Parameters(
        name = "isPartitioned = {0}, " +
            "includeOptionalOptions = {1}, " +
            "useStaticPartition = {2}, " +
            "useBoundedMode = {3}"
    )
    public static Collection<Object[]> params() {
        return Arrays.asList(
            // isPartitioned, includeOptionalOptions, useStaticPartition, useBoundedMode
            new Object[]{false, false, false, false},
            new Object[]{false, true, false, false},
            new Object[]{true, false, false, false},
            new Object[]{true, false, true, false},
            new Object[]{false, false, false, true}
        );
    }

    @Parameterized.Parameter(0)
    public Boolean isPartitioned;

    @Parameterized.Parameter(1)
    public Boolean includeOptionalOptions;

    @Parameterized.Parameter(2)
    public Boolean useStaticPartition;

    @Parameterized.Parameter(3)
    public Boolean useBoundedMode;

    private String deltaTablePath;
    protected RowType testRowType = TEST_ROW_TYPE;

    @Before
    public void setup() {
        try {
            deltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            if (includeOptionalOptions) {
                // one of the optional options is whether the sink should try to update the table's
                // schema, so we are initializing an existing table to test this behaviour
                DeltaSinkTestUtils.initTestForTableApiTable(deltaTablePath);
                testRowType = DeltaSinkTestUtils.addNewColumnToSchema(TEST_ROW_TYPE);
            }
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @Test
    public void testTableApi() throws Exception {
        // GIVEN
        DeltaLog deltaLog = DeltaLog.forTable(DeltaSinkTestUtils.getHadoopConf(), deltaTablePath);
        List<AddFile> initialDeltaFiles = deltaLog.snapshot().getAllFiles();

        // WHEN
        if (useBoundedMode) {
            runFlinkJob();
        } else {
            runFlinkJobInBackground();
        }
        DeltaSinkTestUtils.waitUntilDeltaLogExists(deltaLog, deltaLog.snapshot().getVersion() + 1);

        // THEN
        deltaLog.update();
        int tableRecordsCount =
            TestParquetReader.readAndValidateAllTableRecords(deltaLog, TEST_ROW_TYPE);
        List<AddFile> files = deltaLog.update().getAllFiles();
        assertTrue(files.size() > initialDeltaFiles.size());
        assertTrue(tableRecordsCount > 0);

        if (isPartitioned) {
            assertThat(
                deltaLog.snapshot().getMetadata().getPartitionColumns(),
                CoreMatchers.is(Arrays.asList("col1", "col3")));
        } else {
            assertTrue(deltaLog.snapshot().getMetadata().getPartitionColumns().isEmpty());
        }

        List<String> expectedTableCols = includeOptionalOptions ?
            Arrays.asList("col1", "col2", "col3", "col4") : Arrays.asList("col1", "col2", "col3");
        assertThat(
            Arrays.asList(deltaLog.snapshot().getMetadata().getSchema().getFieldNames()),
            CoreMatchers.is(expectedTableCols));

        if (useStaticPartition) {
            for (AddFile file : deltaLog.snapshot().getAllFiles()) {
                assertEquals("val1", file.getPartitionValues().get("col1"));
            }
        }
    }

    /**
     * Runs Flink job in a daemon thread.
     * <p>
     * This workaround is needed because if we try to first run the Flink job and then query the
     * table with Delta Standalone Reader (DSR) then we are hitting "closes classloader exception"
     * which in short means that finished Flink job closes the classloader for the classes that DSR
     * tries to reuse.
     */
    private void runFlinkJobInBackground() {
        new Thread(() -> {
            try {
                runFlinkJob();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void runFlinkJob() throws ExecutionException, InterruptedException {
        TableEnvironment tableEnv;
        if (useBoundedMode) {
            EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()
                .build();
            tableEnv = TableEnvironment.create(settings);
        } else {
            tableEnv = StreamTableEnvironment.create(getTestStreamEnv());
        }

        final String testSourceTableName = "test_source_table";
        String sourceSql = buildSourceTableSql(testSourceTableName, 10);
        tableEnv.executeSql(sourceSql);

        final String testCompactSinkTableName = "test_compact_sink_table";
        String sinkSql = buildSinkTableSql(testCompactSinkTableName, deltaTablePath);
        tableEnv.executeSql(sinkSql);

        final String sql1 = buildInsertIntoSql(testCompactSinkTableName, testSourceTableName);
        try {
            tableEnv.executeSql(sql1).await();
        } catch (Exception exception) {
            if (!exception.getMessage().contains("Failed to wait job finish")) {
                throw exception;
            }
        }
    }

    private StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

    private String buildSourceTableSql(String testSourceTableName, int rows) {
        String additionalCol = includeOptionalOptions ? ", col4 INT " : "";
        String rowLimit = useBoundedMode ? "'number-of-rows' = '1'," : "";
        return String.format(
            "CREATE TABLE %s ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + additionalCol
                + ") WITH ("
                + " 'connector' = 'datagen',"
                + rowLimit
                + " 'rows-per-second' = '20'"
                + ")",
            testSourceTableName, rows);
    }

    private String buildSinkTableSql(String tableName, String tablePath) {
        String resourcesDirectory = new File("src/test/resources").getAbsolutePath();
        String optionalTableOptions = (includeOptionalOptions ?
            String.format(
                " 'hadoop-conf-dir' = '%s', " +
                    "'merge-schema' = 'true', ",
                resourcesDirectory)
            : ""
        );

        String partitionedClause = isPartitioned ? "PARTITIONED BY (col1, col3) " : "";
        String additionalCol = includeOptionalOptions ? ", col4 INT " : "";

        return String.format(
            "CREATE TABLE %s ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + additionalCol
                + ") "
                + partitionedClause
                + "WITH ("
                + " 'connector' = 'delta',"
                + optionalTableOptions
                + " 'table-path' = '%s'"
                + ")",
            tableName, tablePath);
    }

    private String buildInsertIntoSql(String sinkTable, String sourceTable) {
        if (useStaticPartition) {
            return String.format(
                "INSERT INTO %s PARTITION(col1='val1') " +
                    "SELECT col2, col3 FROM %s", sinkTable, sourceTable);
        }
        return String.format("INSERT INTO %s SELECT * FROM %s", sinkTable, sourceTable);
    }
}
