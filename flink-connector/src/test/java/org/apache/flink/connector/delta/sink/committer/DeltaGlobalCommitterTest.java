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

import org.junit.Test;

/**
 * Tests for {@link DeltaCommitter}.
 */
public class DeltaGlobalCommitterTest {

    @Test
    public void testCommitToDeltaLogAppendMode() throws Exception {

    }

    @Test(expected = RuntimeException.class)
    public void testCommittablesFromDifferentCheckpointIntervalOneWithIncompatiblePartitions()
        throws Exception {
        //GIVEN
        DeltaSinkTestUtils.initTestForPartitionedTable(tablePath.getPath());
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
    public void testCommitToDeltaLogCompleteMode() throws Exception {

    }

    @Test
    public void testPrepareDeltaLogOperation() throws Exception {

    }



}
