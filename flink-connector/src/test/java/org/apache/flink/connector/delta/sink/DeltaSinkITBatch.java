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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.HadoopConfTest;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.TestDeltaLakeTable;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.TestFileSystem;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.TestRowData;
import org.apache.flink.connector.delta.sink.utils.ITCaseUtils;
import org.apache.flink.connector.file.sink.BatchExecutionFileSinkITCase;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.CommitInfo;

public class DeltaSinkITBatch extends BatchExecutionFileSinkITCase {

    private String deltaTablePath;

    @Before
    public void setup() {
        try {
            deltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            TestDeltaLakeTable.initializeTestStateForNonPartitionedDeltaTable(deltaTablePath);
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @Override
    @Test
    public void testFileSink() throws Exception {
        runDeltaSinkTest();
    }

    public void runDeltaSinkTest() throws Exception {
        // GIVEN
        DeltaLog deltaLog = DeltaLog.forTable(HadoopConfTest.getHadoopConf(), deltaTablePath);
        List<AddFile> initialDeltaFiles = deltaLog.snapshot().getAllFiles();
        long initialVersion = deltaLog.snapshot().getVersion();
        assertEquals(initialDeltaFiles.size(), 2);

        JobGraph jobGraph = createJobGraph(deltaTablePath);

        // WHEN
        try (MiniCluster miniCluster = ITCaseUtils.getMiniCluster()) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }

        // THEN
        TestFileSystem.validateIfPathContainsParquetFilesWithData(deltaTablePath);
        int writtenRecordsCount =
            TestFileSystem.validateIfPathContainsParquetFilesWithData(deltaTablePath);
        assertEquals(NUM_RECORDS, writtenRecordsCount - initialDeltaFiles.size());

        List<AddFile> finalDeltaFiles = deltaLog.update().getAllFiles();
        assertTrue(finalDeltaFiles.size() > initialDeltaFiles.size());
        Iterator<Long> it = LongStream.range(
            initialVersion + 1, deltaLog.snapshot().getVersion() + 1).iterator();
        long totalRowsAdded = 0;
        long totalAddedFiles = 0;
        while (it.hasNext()) {
            long currentVersion = it.next();
            CommitInfo currentCommitInfo = deltaLog.getCommitInfoAt(currentVersion);
            Optional<Map<String, String>> operationMetrics =
                currentCommitInfo.getOperationMetrics();
            assertTrue(operationMetrics.isPresent());
            totalRowsAdded += Long.parseLong(operationMetrics.get().get("numOutputRows"));
            totalAddedFiles += Long.parseLong(operationMetrics.get().get("numAddedFiles"));

            assertTrue(Integer.parseInt(operationMetrics.get().get("numOutputBytes")) > 0);
        }

        assertEquals(finalDeltaFiles.size() - initialDeltaFiles.size(), totalAddedFiles);
        assertEquals(NUM_RECORDS, totalRowsAdded);
    }

    @Override
    protected JobGraph createJobGraph(String path) {
        StreamExecutionEnvironment env = getTestStreamEnv();

        env.fromCollection(TestRowData.getTestRowData(NUM_RECORDS))
            .setParallelism(1)
            .sinkTo(ITCaseUtils.createDeltaSink(path))
            .setParallelism(NUM_SINKS);

        StreamGraph streamGraph = env.getStreamGraph();
        return streamGraph.getJobGraph();
    }

    private StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(config, getClass().getClassLoader());
        return env;
    }
}
