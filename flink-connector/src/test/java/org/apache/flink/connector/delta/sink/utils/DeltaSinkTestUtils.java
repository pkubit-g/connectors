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

package org.apache.flink.connector.delta.sink.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaPendingFile;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DeltaSinkTestUtils {

    public static class TestRowData {

        public static final RowType TEST_ROW_TYPE = new RowType(Arrays.asList(
            new RowType.RowField("name", new VarCharType(VarCharType.MAX_LENGTH)),
            new RowType.RowField("surname", new VarCharType(VarCharType.MAX_LENGTH)),
            new RowType.RowField("age", new IntType())
        ));

        public static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER =
            DataFormatConverters.getConverterForDataType(
                TypeConversions.fromLogicalToDataType(TEST_ROW_TYPE)
            );

        public static List<RowData> getTestRowData(int num_records) {
            List<RowData> rows = new ArrayList<>(num_records);
            for (int i = 0; i < num_records; i++) {
                Integer v = i;
                rows.add(
                    CONVERTER.toInternal(
                        Row.of(
                            String.valueOf(v),
                            String.valueOf((v + v)),
                            v)
                    )
                );
            }
            return rows;
        }

        public static RowData getTestRowDataEvent(String name,
                                                  String surname,
                                                  Integer age) {
            return CONVERTER.toInternal(Row.of(name, surname, age));
        }
    }

    public static class TestDeltaLakeTable {

        public static LinkedHashMap<String, String> getEmptyTestPartitionSpec() {
            return new LinkedHashMap<>();
        }

        public static LinkedHashMap<String, String> getTestPartitionSpec() {
            return new LinkedHashMap<String, String>() {{
                put("a", "b");
                put("c", "d");
            }};
        }

        public static final String TEST_DELTA_TABLE_INITIAL_STATE_NP_DIR =
            "test-data/test-non-partitioned-delta-table-initial-state";
        public static final String TEST_DELTA_TABLE_INITIAL_STATE_NP_FULL_PATH =
            TestDeltaLakeTable.class
                .getClassLoader()
                .getResource(TEST_DELTA_TABLE_INITIAL_STATE_NP_DIR)
                .getPath();
        public static final String TEST_DELTA_TABLE_INITIAL_STATE_P_DIR =
            "test-data/test-partitioned-delta-table-initial-state";
        public static final String TEST_DELTA_TABLE_INITIAL_STATE_P_FULL_PATH =
            TestDeltaLakeTable.class
                .getClassLoader()
                .getResource(TEST_DELTA_TABLE_INITIAL_STATE_P_DIR)
                .getPath();

        public static void initializeTestStateForNonPartitionedDeltaTable(String targetTablePath)
            throws IOException {
            FileUtils.copyDirectory(
                new File(TEST_DELTA_TABLE_INITIAL_STATE_NP_FULL_PATH),
                new File(targetTablePath));
        }

        public static void initializeTestStateForPartitionedDeltaTable(String targetTablePath)
            throws IOException {
            FileUtils.copyDirectory(
                new File(TEST_DELTA_TABLE_INITIAL_STATE_P_FULL_PATH),
                new File(targetTablePath));
        }
    }

    public static class TestDeltaPendingFile {
        public static DeltaPendingFile getTestDeltaPendingFile() {
            return getTestDeltaPendingFile(new LinkedHashMap<>());
        }

        public static DeltaPendingFile getTestDeltaPendingFile(
            LinkedHashMap<String, String> partitionSpec) {
            return new DeltaPendingFile(
                partitionSpec,
                "file_name-" + UUID.randomUUID(),
                new FileSinkTestUtils.TestPendingFileRecoverable(),
                new Random().nextInt(30000),
                new Random().nextInt(500000),
                System.currentTimeMillis()
            );
        }
    }

    public static class TestDeltaCommittable {

        static final String TEST_APP_ID = UUID.randomUUID().toString();
        static final long TEST_CHECKPOINT_ID = new Random().nextInt(10);

        public static List<DeltaCommittable> getListOfDeltaCommittables(
            int size, LinkedHashMap<String, String> partitionSpec) {
            return getListOfDeltaCommittables(size, partitionSpec, TEST_CHECKPOINT_ID);
        }

        public static List<DeltaCommittable> getListOfDeltaCommittables(
            int size, LinkedHashMap<String, String> partitionSpec, long checkpointId) {
            List<DeltaCommittable> deltaCommittableList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                deltaCommittableList.add(
                    TestDeltaCommittable.getTestDeltaCommittableWithPendingFile(
                        partitionSpec, checkpointId)
                );
            }
            return deltaCommittableList;
        }

        public static DeltaCommittable getTestDeltaCommittableWithPendingFile(
            LinkedHashMap<String, String> partitionSpec) {
            return getTestDeltaCommittableWithPendingFile(partitionSpec, TEST_CHECKPOINT_ID);
        }

        public static DeltaCommittable getTestDeltaCommittableWithPendingFile(
            LinkedHashMap<String, String> partitionSpec, long checkpointId) {
            return new DeltaCommittable(
                TestDeltaPendingFile.getTestDeltaPendingFile(partitionSpec),
                TEST_APP_ID,
                checkpointId
            );
        }

        public static void validateDeltaCommittablesEquality(
            DeltaCommittable committable,
            DeltaCommittable deserialized,
            LinkedHashMap<String, String> expectedPartitionSpec) {
            assertEquals(
                committable.getDeltaPendingFile().getPendingFile(),
                deserialized.getDeltaPendingFile().getPendingFile());
            assertEquals(committable.getCheckpointId(), deserialized.getCheckpointId());
            assertEquals(committable.getAppId(), deserialized.getAppId());
            assertEquals(
                committable.getDeltaPendingFile().getFileName(),
                deserialized.getDeltaPendingFile().getFileName());
            assertEquals(
                committable.getDeltaPendingFile().getFileSize(),
                deserialized.getDeltaPendingFile().getFileSize());
            assertEquals(
                committable.getDeltaPendingFile().getRecordCount(),
                deserialized.getDeltaPendingFile().getRecordCount());
            assertEquals(
                committable.getDeltaPendingFile().getLastUpdateTime(),
                deserialized.getDeltaPendingFile().getLastUpdateTime());
            assertEquals(
                expectedPartitionSpec,
                deserialized.getDeltaPendingFile().getPartitionSpec());
        }
    }

    public static class HadoopConfTest {
        public static org.apache.hadoop.conf.Configuration getHadoopConf() {
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            conf.set("parquet.compression", "SNAPPY");
            return conf;
        }
    }

    public static class TestFileSystem {
        public static int validateIfPathContainsParquetFilesWithData(String deltaTablePath)
            throws IOException {
            List<File> files =
                Stream.of(Objects.requireNonNull(new File(deltaTablePath).listFiles()))
                    .filter(file -> !file.isDirectory())
                    .filter(file -> !file.getName().contains("inprogress"))
                    .filter(file -> file.getName().endsWith(".snappy.parquet"))
                    .collect(Collectors.toList());

            assertTrue(files.size() > 0);

            int totalRecordsCount = 0;
            for (File file : files) {
                // simple check if files contain any data besides footer
                assertTrue(file.length() > 100);
                totalRecordsCount += TestParquetReader.readAndParseRecords(
                    new Path(file.toURI()),
                    TestRowData.TEST_ROW_TYPE);
            }
            return totalRecordsCount;
        }
    }

    public static class TestParquetWriterFactory {

        public static ParquetWriterFactory<RowData> createTestWriterFactory() {
            return ParquetRowDataBuilder.createWriterFactory(
                TestRowData.TEST_ROW_TYPE, HadoopConfTest.getHadoopConf(), true);
        }
    }
}
