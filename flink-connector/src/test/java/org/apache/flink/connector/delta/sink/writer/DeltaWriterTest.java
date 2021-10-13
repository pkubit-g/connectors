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

import org.junit.Test;

import java.io.IOException;

/**
 * Tests for {@link DeltaWriter}.
 */
@RunWith(Parameterized.class)
public class DeltaWriterTest {


    @Parameterized.Parameters(
        name = "isPartitioned = {0}"
    )
    public static Collection<Object[]> params() {
        return Arrays.asList(
            new Object[]{false},
            new Object[]{true}
        );
    }

    @Parameterized.Parameter(0)
    public Boolean isPartitioned;

    // counter for the records produced by given test instance
    private int testRecordsCount = 0;

    @Test
    public void testPreCommit() throws Exception {

    }

    @Test
    public void testSnapshotAndRestore() throws Exception {

    }

    @Test
    public void testMergingForRescaling() throws Exception {

    }

    @Test
    public void testBucketIsRemovedWhenNotActive() throws Exception {

    }

    @Test
    public void testOnProcessingTime() throws IOException, InterruptedException {

    }

    @Test
    public void testContextPassingNormalExecution() throws Exception {

    }

    @Test
    public void testContextPassingNullTimestamp() throws Exception {

    }

}
