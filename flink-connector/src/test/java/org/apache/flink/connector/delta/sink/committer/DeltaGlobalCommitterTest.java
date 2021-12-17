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

package org.apache.flink.connector.delta.sink.committer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.flink.connector.delta.sink.SchemaConverter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittableSerializer;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;

/**
 * Tests for {@link DeltaGlobalCommitter}.
 */
public class DeltaGlobalCommitterTest {

    private final String TEST_APP_ID = UUID.randomUUID().toString();

    private final long TEST_CHECKPOINT_ID = new Random().nextInt(10);

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private Path tablePath;

    @Before
    public void setup() throws IOException {
        tablePath = new Path(TEMPORARY_FOLDER.newFolder().toURI());

    }

    @Test
    public void testCommitTwice() throws Exception {
        //GIVEN
        int numAddedFiles = 3;
        DeltaSinkTestUtils.initializeTestStateForPartitionedDeltaTable(tablePath.getPath());
        DeltaLog deltaLog = DeltaLog.forTable(
            DeltaSinkTestUtils.getHadoopConf(), tablePath.getPath());
        assertEquals(deltaLog.snapshot().getVersion(), 0);
        int initialTableFilesCount = deltaLog.snapshot().getAllFiles().size();

        List<DeltaCommittable> deltaCommittables = DeltaSinkTestUtils.getListOfDeltaCommittables(
            numAddedFiles, DeltaSinkTestUtils.getTestPartitionSpec());
        List<DeltaGlobalCommittable> globalCommittables =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        DeltaGlobalCommitter globalCommitter =
            getTestGlobalCommitter(DeltaSinkTestUtils.TEST_ROW_TYPE);

        // WHEN
        globalCommitter.commit(globalCommittables);
        deltaLog.update();
        assertEquals(deltaLog.snapshot().getVersion(), 1);
        globalCommitter.commit(globalCommittables);

        // THEN
        // after trying to commit same committables nothing should change in DeltaLog
        deltaLog.update();
        assertEquals(1, deltaLog.snapshot().getVersion());
        assertEquals(
            initialTableFilesCount + numAddedFiles,
            deltaLog.snapshot().getAllFiles().size());
    }

    @Test
    public void testCanTryUpdateSchemaSetToTrue() throws Exception {
        //GIVEN
        DeltaSinkTestUtils.initializeTestStateForPartitionedDeltaTable(tablePath.getPath());
        DeltaLog deltaLog = DeltaLog.forTable(
            DeltaSinkTestUtils.getHadoopConf(), tablePath.getPath());

        List<DeltaCommittable> deltaCommittables = DeltaSinkTestUtils.getListOfDeltaCommittables(
            3, DeltaSinkTestUtils.getTestPartitionSpec());
        List<DeltaGlobalCommittable> globalCommittables =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        // add new field to the schema
        RowType updatedSchema = DeltaSinkTestUtils.addNewColumnToSchema();

        DeltaGlobalCommitter globalCommitter = new DeltaGlobalCommitter(
            DeltaSinkTestUtils.getHadoopConf(),
            tablePath,
            updatedSchema,
            true // canTryUpdateSchema
        );

        // WHEN
        globalCommitter.commit(globalCommittables);

        // THEN
        // schema before deltaLog.update() is in old format, but after update it equals to the new
        // format
        assertEquals(deltaLog.snapshot().getMetadata().getSchema().toJson(),
            new SchemaConverter().toDeltaFormat(DeltaSinkTestUtils.TEST_ROW_TYPE).toJson());
        deltaLog.update();
        assertEquals(deltaLog.snapshot().getMetadata().getSchema().toJson(),
            new SchemaConverter().toDeltaFormat(updatedSchema).toJson());
    }

    @Test(expected = RuntimeException.class)
    public void testCanTryUpdateSchemaSetToFalse() throws Exception {
        //GIVEN
        DeltaSinkTestUtils.initializeTestStateForPartitionedDeltaTable(tablePath.getPath());

        List<DeltaCommittable> deltaCommittables = DeltaSinkTestUtils.getListOfDeltaCommittables(
            3, DeltaSinkTestUtils.getTestPartitionSpec());
        List<DeltaGlobalCommittable> globalCommittables =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        // new schema drops one of the previous columns
        RowType updatedSchema = DeltaSinkTestUtils.dropOneColumnFromSchema();
        DeltaGlobalCommitter globalCommitter = getTestGlobalCommitter(updatedSchema);

        // WHEN
        globalCommitter.commit(globalCommittables);
    }

    @Test(expected = RuntimeException.class)
    public void testCanTryUpdateIncompatibleSchema() throws Exception {
        //GIVEN
        DeltaSinkTestUtils.initializeTestStateForPartitionedDeltaTable(tablePath.getPath());

        List<DeltaCommittable> deltaCommittables = DeltaSinkTestUtils.getListOfDeltaCommittables(
            3, DeltaSinkTestUtils.getTestPartitionSpec());
        List<DeltaGlobalCommittable> globalCommittables =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        // new schema drops one of the previous columns
        RowType updatedSchema = DeltaSinkTestUtils.dropOneColumnFromSchema();

        DeltaGlobalCommitter globalCommitter = getTestGlobalCommitter(updatedSchema);

        // WHEN
        globalCommitter.commit(globalCommittables);
    }

    @Test(expected = RuntimeException.class)
    public void testWrongStreamPartitionValues() throws Exception {
        //GIVEN
        DeltaSinkTestUtils.initializeTestStateForPartitionedDeltaTable(tablePath.getPath());

        List<DeltaCommittable> deltaCommittables = DeltaSinkTestUtils.getListOfDeltaCommittables(
            1, getNonMatchingPartitionSpec());
        List<DeltaGlobalCommittable> globalCommittables =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        DeltaGlobalCommitter globalCommitter =
            getTestGlobalCommitter(DeltaSinkTestUtils.TEST_ROW_TYPE);

        // WHEN
        globalCommitter.commit(globalCommittables);
    }

    @Test
    public void testCommittablesFromDifferentCheckpointInterval() {
        //GIVEN
        int numAddedFiles1 = 3;
        int numAddedFiles2 = 5;
        DeltaLog deltaLog = DeltaLog.forTable(
            DeltaSinkTestUtils.getHadoopConf(), tablePath.getPath());
        int initialTableFilesCount = deltaLog.snapshot().getAllFiles().size();
        assertEquals(-1, deltaLog.snapshot().getVersion());

        List<DeltaCommittable> deltaCommittables = DeltaSinkTestUtils.getListOfDeltaCommittables(
            numAddedFiles1, new LinkedHashMap<>(), 1);
        deltaCommittables.addAll(DeltaSinkTestUtils.getListOfDeltaCommittables(
            numAddedFiles2, new LinkedHashMap<>(), 2));
        List<DeltaGlobalCommittable> globalCommittables =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        DeltaGlobalCommitter globalCommitter =
            getTestGlobalCommitter(DeltaSinkTestUtils.TEST_ROW_TYPE);

        // WHEN
        globalCommitter.commit(globalCommittables);

        // THEN
        // we should have committed both checkpoints intervals so current snapshot version should
        // be 1 and should contain files from both intervals.
        deltaLog.update();
        assertEquals(1, deltaLog.snapshot().getVersion());
        assertEquals(
            initialTableFilesCount + numAddedFiles1 + numAddedFiles2,
            deltaLog.snapshot().getAllFiles().size());
    }

    @Test
    public void testCommittablesFromDifferentCheckpointIntervalOneOutdated() throws IOException {
        // GIVEN
        // although it does not make any sense for real world scenarios that the retried set of
        // committables is different from the previous one however for this test it better to
        // differentiate those by changing the number of files to commit which will make the final
        // validation unambiguous
        int numAddedFiles1FirstTrial = 3;
        int numAddedFiles1SecondTrial = 4;
        int numAddedFiles2 = 10;
        DeltaLog deltaLog = DeltaLog.forTable(
            DeltaSinkTestUtils.getHadoopConf(), tablePath.getPath());
        assertEquals(-1, deltaLog.snapshot().getVersion());

        List<DeltaCommittable> deltaCommittables1FirstTrial =
            DeltaSinkTestUtils.getListOfDeltaCommittables(
                numAddedFiles1FirstTrial, new LinkedHashMap<>(), 1);
        List<DeltaCommittable> deltaCommittables1SecondTrial =
            DeltaSinkTestUtils.getListOfDeltaCommittables(
                numAddedFiles1SecondTrial, new LinkedHashMap<>(), 1);
        List<DeltaCommittable> deltaCommittables2 = DeltaSinkTestUtils.getListOfDeltaCommittables(
            numAddedFiles2, new LinkedHashMap<>(), 2);
        List<DeltaCommittable> deltaCommittablesCombined = new ArrayList<>(Collections.emptyList());
        deltaCommittablesCombined.addAll(deltaCommittables1SecondTrial);
        deltaCommittablesCombined.addAll(deltaCommittables2);

        List<DeltaGlobalCommittable> globalCommittables1FirstTrial =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables1FirstTrial));
        List<DeltaGlobalCommittable> globalCommittablesCombined =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittablesCombined));

        DeltaGlobalCommitter globalCommitter =
            getTestGlobalCommitter(DeltaSinkTestUtils.TEST_ROW_TYPE);

        // WHEN
        // we first commit committables from the former checkpoint interval, and then combined
        // committables from both checkpoint intervals
        globalCommitter.commit(globalCommittables1FirstTrial);
        globalCommitter.commit(globalCommittablesCombined);

        // THEN
        // we should've committed only files from the first try for checkpointId == 1 and files
        // for checkpointId == 2
        deltaLog.update();
        assertEquals(1, deltaLog.snapshot().getVersion());
        List<AddFile> filesInTable = deltaLog.snapshot().getAllFiles();
        assertEquals(numAddedFiles1FirstTrial + numAddedFiles2, filesInTable.size());

        // we simply check if the table really contains all the files from the first trial
        // and by implication it will also mean that it does not contain any files from second trial
        for (DeltaCommittable deltaCommittable : deltaCommittables1FirstTrial) {
            Stream<String> filePathsInTableStream = filesInTable
                .stream()
                .map(AddFile::getPath);
            assertTrue(
                filePathsInTableStream.anyMatch(name -> {
                    String name1 = deltaCommittable.getDeltaPendingFile().getFileName();
                    return name.equals(name1);
                })
            );
        }
    }

    @Test(expected = RuntimeException.class)
    public void testCommittablesFromDifferentCheckpointIntervalOneWithIncompatiblePartitions()
        throws Exception {
        //GIVEN
        DeltaSinkTestUtils.initializeTestStateForPartitionedDeltaTable(tablePath.getPath());
        int numAddedFiles1 = 3;
        int numAddedFiles2 = 5;
        DeltaLog deltaLog = DeltaLog.forTable(
            DeltaSinkTestUtils.getHadoopConf(), tablePath.getPath());
        assertEquals(0, deltaLog.snapshot().getVersion());
        int initialNumberOfFiles = deltaLog.snapshot().getAllFiles().size();

        List<DeltaCommittable> deltaCommittables1 = DeltaSinkTestUtils.getListOfDeltaCommittables(
            numAddedFiles1, DeltaSinkTestUtils.getTestPartitionSpec(), 1);
        List<DeltaCommittable> deltaCommittables2 = DeltaSinkTestUtils.getListOfDeltaCommittables(
            numAddedFiles2, getNonMatchingPartitionSpec(), 2);

        List<DeltaGlobalCommittable> globalCommittables = Arrays.asList(
            new DeltaGlobalCommittable(deltaCommittables1),
            new DeltaGlobalCommittable(deltaCommittables2)
        );

        DeltaGlobalCommitter globalCommitter =
            getTestGlobalCommitter(DeltaSinkTestUtils.TEST_ROW_TYPE);

        // WHEN
        try {
            globalCommitter.commit(globalCommittables);
        } catch (Exception exc) {
            // the commit should raise an exception for incompatible committables for the second
            // checkpoint interval but correct committables for the first checkpoint interval should
            // have been committed
            deltaLog.update();
            assertEquals(1, deltaLog.snapshot().getVersion());
            assertEquals(
                initialNumberOfFiles + numAddedFiles1, deltaLog.snapshot().getAllFiles().size());
            // we rethrow the exception for the test to pass
            throw exc;
        }
    }

    @Test
    public void testGlobalCommittableSerializerWithCommittables() throws IOException {
        // GIVEN
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
        partitionSpec.put("a", "b");
        partitionSpec.put("c", "d");

        List<DeltaCommittable> deltaCommittables = Arrays.asList(
            new DeltaCommittable(
                DeltaSinkTestUtils.getTestDeltaPendingFile(partitionSpec),
                TEST_APP_ID,
                TEST_CHECKPOINT_ID),
            new DeltaCommittable(
                DeltaSinkTestUtils.getTestDeltaPendingFile(partitionSpec),
                TEST_APP_ID,
                TEST_CHECKPOINT_ID + 1)
        );
        DeltaGlobalCommittable globalCommittable = new DeltaGlobalCommittable(deltaCommittables);

        // WHEN
        DeltaGlobalCommittable deserialized = serializeAndDeserialize(globalCommittable);

        // THEN
        for (int i = 0; i < deserialized.getDeltaCommittables().size(); i++) {
            DeltaSinkTestUtils.validateDeltaCommittablesEquality(
                globalCommittable.getDeltaCommittables().get(i),
                deserialized.getDeltaCommittables().get(i),
                partitionSpec
            );
        }
    }

    @Test
    public void testGlobalCommittableSerializerWithEmptyCommittables() throws IOException {
        // GIVEN
        DeltaGlobalCommittable globalCommittable = new DeltaGlobalCommittable(new ArrayList<>());

        // WHEN
        DeltaGlobalCommittable deserialized = serializeAndDeserialize(globalCommittable);

        // THEN
        assertTrue(globalCommittable.getDeltaCommittables().isEmpty());
        assertTrue(deserialized.getDeltaCommittables().isEmpty());
    }

    ///////////////////////////////////////////////////
    // test method utils
    ///////////////////////////////////////////////////

    private DeltaGlobalCommitter getTestGlobalCommitter(RowType schema) {
        return new DeltaGlobalCommitter(
            DeltaSinkTestUtils.getHadoopConf(),
            tablePath,
            schema,
            false // canTryUpdateSchema
        );
    }

    private LinkedHashMap<String, String> getNonMatchingPartitionSpec() {
        LinkedHashMap<String, String> nonMatchingPartitionSpec =
            DeltaSinkTestUtils.getTestPartitionSpec();
        nonMatchingPartitionSpec.remove(nonMatchingPartitionSpec.keySet().toArray()[0]);
        return nonMatchingPartitionSpec;
    }

    ///////////////////////////////////////////////////
    // serde test utils
    ///////////////////////////////////////////////////

    private DeltaGlobalCommittable serializeAndDeserialize(DeltaGlobalCommittable globalCommittable)
        throws IOException {
        DeltaGlobalCommittableSerializer serializer =
            new DeltaGlobalCommittableSerializer(
                new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                    FileSinkTestUtils.TestPendingFileRecoverable::new)
            );
        byte[] data = serializer.serialize(globalCommittable);
        return serializer.deserialize(serializer.getVersion(), data);
    }
}
