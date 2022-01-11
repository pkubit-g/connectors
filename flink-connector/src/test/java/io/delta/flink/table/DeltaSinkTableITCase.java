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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.sink.utils.TestParquetReader;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;

public class DeltaSinkTableITCase {

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    public static final RowType TEST_ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("col1", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("col2", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("col3", new IntType())
    ));

    private String deltaTablePath;
    protected StreamExecutionEnvironment streamEnv;
    protected StreamTableEnvironment tableEnv;

    @Before
    public void setup() {
        try {
            deltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @Test
    public void testTableApi() throws Exception {
        // GIVEN
        DeltaLog deltaLog = DeltaLog.forTable(DeltaSinkTestUtils.getHadoopConf(), deltaTablePath);
        List<AddFile> initialDeltaFiles = deltaLog.snapshot().getAllFiles();
        assertEquals(initialDeltaFiles.size(), 0);
        long initialVersion = deltaLog.snapshot().getVersion();
        assertEquals(initialVersion, -1);

        // WHEN
        runFlinkJobInBackground();
        DeltaSinkTestUtils.waitUntilDeltaLogExists(deltaLog);

        // THEN
        deltaLog.update();
        int tableRecordsCount =
            TestParquetReader.readAndValidateAllTableRecords(deltaLog, TEST_ROW_TYPE);
        List<AddFile> files = deltaLog.update().getAllFiles();
        assertTrue(files.size() > initialDeltaFiles.size());
        assertTrue(tableRecordsCount > 0);
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
        streamEnv = getTestStreamEnv();
        tableEnv = StreamTableEnvironment.create(streamEnv);

        final String testSourceTableName = "test_source_table";
        String sourceSql = buildSourceTableSql(testSourceTableName, 10);
        tableEnv.executeSql(sourceSql);

        final String testCompactSinkTableName = "test_compact_sink_table";
        String sinkSql = buildSinkTableSql(testCompactSinkTableName, deltaTablePath);
        tableEnv.executeSql(sinkSql);

        final String sql1 = buildInsertIntoSql(testCompactSinkTableName, testSourceTableName);
        tableEnv.executeSql(sql1).await();
    }

    private static StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

    private static String buildSourceTableSql(String testSourceTableName, int rows) {
        return String.format(
            "CREATE TABLE %s ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") WITH ("
                + " 'connector' = 'datagen',"
                + " 'rows-per-second' = '1'"
                + ")",
            testSourceTableName, rows);
    }

    private static String buildSinkTableSql(
        String tableName, String tablePath) {
        return String.format(
            "CREATE TABLE %s ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") PARTITIONED BY (col1, col3) WITH ("
                + " 'connector' = 'deltalake',"
                + " 'table-path' = '%s'"
                + ")",
            tableName, tablePath);
    }

    private static String buildInsertIntoSql(String sinkTable, String sourceTable) {
        return String.format("INSERT INTO %s SELECT * FROM %s", sinkTable, sourceTable);
    }
}
