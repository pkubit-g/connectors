package org.apache.flink.connector.delta.sink.utils;

import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaPendingFile;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class DeltaSinkTestUtils {

    public static class TestDeltaLakeTable {

        public static final RowType TEST_ROW_TYPE = new RowType(Arrays.asList(
                new RowType.RowField("name", new VarCharType(VarCharType.MAX_LENGTH)),
                new RowType.RowField("surname", new VarCharType(VarCharType.MAX_LENGTH)),
                new RowType.RowField("age", new IntType())
        ));

    }

    public static class TestDeltaPendingFile {
        public static DeltaPendingFile getTestDeltaPendingFile(LinkedHashMap<String, String> partitionSpec) {
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

        public static List<DeltaCommittable> getListOfDeltaCommittables(int size,
                                                                        LinkedHashMap<String, String> partitionSpec) {
            List<DeltaCommittable> deltaCommittableList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                deltaCommittableList.add(
                        TestDeltaCommittable.getTestDeltaCommittableWithPendingFile(partitionSpec)
                );
            }
            return deltaCommittableList;
        }

        public static DeltaCommittable getTestDeltaCommittableWithPendingFile(LinkedHashMap<String, String> partitionSpec) {
            return new DeltaCommittable(
                    TestDeltaPendingFile.getTestDeltaPendingFile(partitionSpec)
            );
        }

        public static void validateDeltaCommittablesEquality(DeltaCommittable committable,
                                                             DeltaCommittable deserialized,
                                                             LinkedHashMap<String, String> expectedPartitionSpec) {
            assertEquals(committable.getDeltaPendingFile().getPendingFile(), deserialized.getDeltaPendingFile().getPendingFile());
            assertEquals(committable.getDeltaPendingFile().getFileName(), deserialized.getDeltaPendingFile().getFileName());
            assertEquals(committable.getDeltaPendingFile().getFileSize(), deserialized.getDeltaPendingFile().getFileSize());
            assertEquals(committable.getDeltaPendingFile().getRecordCount(), deserialized.getDeltaPendingFile().getRecordCount());
            assertEquals(committable.getDeltaPendingFile().getLastUpdateTime(), deserialized.getDeltaPendingFile().getLastUpdateTime());
            assertEquals(expectedPartitionSpec, deserialized.getDeltaPendingFile().getPartitionSpec());
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
        public static void validateIfPathContainsParquetFilesWithData(String deltaTablePath) {
            List<File> files = Stream.of(Objects.requireNonNull(new File(deltaTablePath).listFiles()))
                    .filter(file -> !file.isDirectory())
                    .filter(file -> !file.getName().contains("inprogress"))
                    .filter(file -> file.getName().endsWith(".snappy.parquet"))
                    .collect(Collectors.toList());

            assert files.size() > 0;

            for (File file : files) {
                assert file.length() > 100; // simple check if files contain any data
            }
        }
    }

}
