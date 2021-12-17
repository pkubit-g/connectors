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

package org.apache.flink.connector.delta.sink.writer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committer.DeltaCommitter;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.TestRowData;
import org.apache.flink.connector.delta.sink.utils.TestParquetReader;
import org.apache.flink.connector.delta.sink.utils.WriterTestUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.data.RowData;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link DeltaWriterBucket}.
 */
public class DeltaWriterBucketTest {

    @ClassRule
    public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
    private static final String BUCKET_ID = "testing-bucket";
    private static final String APP_ID = "1";

    @Test
    public void testOnCheckpointNoPendingRecoverable() throws IOException {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path bucketPath = new Path(outDir.toURI());
        DeltaWriterBucket<RowData> bucketWriter = getBucketWriter(bucketPath);

        // WHEN
        List<DeltaCommittable> deltaCommittables =
            onCheckpointActions(bucketWriter, bucketPath, false);

        // THEN
        assertEquals(0, deltaCommittables.size());
    }

    @Test
    public void testOnSingleCheckpointInterval() throws IOException {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path bucketPath = new Path(outDir.toURI());
        int rowsCount = 2;
        List<RowData> testRows = TestRowData.getTestRowData(rowsCount);

        DeltaWriterBucket<RowData> bucketWriter = getBucketWriter(bucketPath);

        // WHEN
        writeData(bucketWriter, testRows);
        List<DeltaCommittable> deltaCommittables =
            onCheckpointActions(bucketWriter, bucketPath, true);

        // THEN
        assertEquals(deltaCommittables.size(), 1);
        int writtenRecordsCount = getWrittenRecordsCount(deltaCommittables, bucketPath);
        assertEquals(rowsCount, writtenRecordsCount);
    }

    @Test
    public void testOnMultipleCheckpointIntervals() throws IOException {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path bucketPath = new Path(outDir.toURI());
        int rowsCount = 2;
        List<RowData> testRows = TestRowData.getTestRowData(rowsCount);

        DeltaWriterBucket<RowData> bucketWriter = getBucketWriter(bucketPath);

        // WHEN
        writeData(bucketWriter, testRows);
        List<DeltaCommittable> deltaCommittables1 =
            onCheckpointActions(bucketWriter, bucketPath, true);

        writeData(bucketWriter, testRows);
        List<DeltaCommittable> deltaCommittables2 =
            onCheckpointActions(bucketWriter, bucketPath, true);

        // THEN
        assertEquals(deltaCommittables1.size(), 1);
        assertEquals(deltaCommittables2.size(), 1);
        List<DeltaCommittable> combinedCommittables =
            Stream.concat(deltaCommittables1.stream(), deltaCommittables2.stream())
                .collect(Collectors.toList());
        int writtenRecordsCount = getWrittenRecordsCount(combinedCommittables, bucketPath);
        assertEquals(rowsCount * 2, writtenRecordsCount);
    }

    /**
     * This test forces one of the pending file to be rolled before checkpoint and then validates
     * that more than one committable (corresponding to one written file) have been generated.
     */
    @Test
    public void testCheckpointWithMultipleRolledFiles() throws IOException {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path bucketPath = new Path(outDir.toURI());
        int rowsCount = 4;
        List<RowData> testRows = TestRowData.getTestRowData(rowsCount);

        DeltaWriterBucket<RowData> bucketWriter = getBucketWriter(
            bucketPath,
            new TestForcedRollFilePolicy());

        // WHEN
        // writing 4 rows, while only the second one should force rolling
        writeData(bucketWriter, testRows);
        List<DeltaCommittable> deltaCommittables =
            onCheckpointActions(bucketWriter, bucketPath, true);

        // THEN
        assertEquals(
            "Two files should have been rolled during tested checkpoint interval",
            deltaCommittables.size(),
            2
        );
        int writtenRecordsCount = getWrittenRecordsCount(deltaCommittables, bucketPath);
        assertEquals(rowsCount, writtenRecordsCount);
    }

    @Test(expected = FileNotFoundException.class)
    public void testCannotReadUncommittedFiles() throws IOException {
        // GIVEN
        File outDir = TEMP_FOLDER.newFolder();
        Path bucketPath = new Path(outDir.toURI());
        int rowsCount = 2;
        List<RowData> testRows = TestRowData.getTestRowData(rowsCount);

        DeltaWriterBucket<RowData> bucketWriter = getBucketWriter(bucketPath);

        // WHEN
        writeData(bucketWriter, testRows);
        List<DeltaCommittable> deltaCommittables =
            onCheckpointActions(bucketWriter, bucketPath, false);

        // THEN
        assertEquals(deltaCommittables.size(), 1);
        getWrittenRecordsCount(deltaCommittables, bucketPath);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Utility Methods
    ///////////////////////////////////////////////////////////////////////////

    private static DeltaWriterBucket<RowData> getBucketWriter(
        Path bucketPath,
        CheckpointRollingPolicy<RowData, String> rollingPolicy) throws IOException {
        return DeltaWriterBucket.DeltaWriterBucketFactory.getNewBucket(
            BUCKET_ID,
            bucketPath,
            WriterTestUtils.createBucketWriter(bucketPath),
            rollingPolicy,
            OutputFileConfig.builder().withPartSuffix(".snappy.parquet").build()
        );
    }

    private static DeltaWriterBucket<RowData> getBucketWriter(Path bucketPath) throws IOException {
        return getBucketWriter(bucketPath, WriterTestUtils.ON_CHECKPOINT_ROLLING_POLICY);
    }

    private static List<DeltaCommittable> onCheckpointActions(DeltaWriterBucket<RowData> bucket,
                                                              Path bucketPath,
                                                              boolean doCommit) throws IOException {
        List<DeltaCommittable> deltaCommittables = bucket.prepareCommit(false, APP_ID, 1);
        DeltaWriterBucketState bucketState = bucket.snapshotState(APP_ID, 1);

        assertEquals(BUCKET_ID, bucketState.getBucketId());
        assertEquals(bucketPath, bucketState.getBucketPath());

        if (doCommit) {
            new DeltaCommitter(
                WriterTestUtils.createBucketWriter(bucketPath)).commit(deltaCommittables);
        }
        return deltaCommittables;
    }

    private static void writeData(DeltaWriterBucket<RowData> bucket,
                                  List<RowData> rows) {
        rows.forEach(rowData -> {
            try {
                bucket.write(rowData, 0);
            } catch (IOException e) {
                throw new RuntimeException("Writing to the bucket failed");
            }
        });
    }

    private static int getWrittenRecordsCount(List<DeltaCommittable> committables,
                                              Path bucketPath) throws IOException {
        int writtenRecordsCount = 0;
        for (DeltaCommittable committable : committables) {
            Path filePath = new Path(bucketPath, committable.getDeltaPendingFile().getFileName());
            writtenRecordsCount +=
                TestParquetReader.readAndParseRecords(filePath, TestRowData.TEST_ROW_TYPE);
        }
        return writtenRecordsCount;
    }

    private static class TestForcedRollFilePolicy extends CheckpointRollingPolicy<RowData, String> {

        /**
         * Forcing second row to roll current in-progress file.
         * See {@link TestRowData#getTestRowData} for reference on the incrementing logic of the
         * test rows.
         */
        @Override
        public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, RowData element) {
            return element.getString(0).toString().equals("1");
        }

        @Override
        public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileState,
                                                  long currentTime) {
            return false;
        }
    }
}
