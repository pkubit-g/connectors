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

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Tests for {@link DeltaWriterBucket}.
 */
public class DeltaWriterBucketTest {

    @ClassRule
    public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Test
    public void testOnCheckpointNoPendingRecoverable() throws IOException {

    }

    @Test
    public void testOnCheckpointRollingOnCheckpoint() throws IOException {

    }

    @Test
    public void testOnCheckpointMultiplePendingFiles() throws IOException {

    }

    @Test
    public void testOnCheckpointWithInProgressFileToCleanup() throws IOException {

    }

    @Test
    public void testFlush() throws IOException {

    }

    @Test
    public void testRollingOnProcessingTime() throws IOException {

    }

    // --------------------------- Checking Restore ---------------------------
    @Test
    public void testRestoreWithInprogressFileNotSupportResume() throws IOException {

    }

    @Test
    public void testRestoreWithInprogressFileSupportResume() throws IOException {

    }


    @Test
    public void testMergeWithInprogressFileNotSupportResume() throws IOException {

    }

    @Test
    public void testMergeWithInprogressFileSupportResume() throws IOException {

    }

}
