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

import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class DeltaGlobalCommitter implements GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable> {

    public DeltaGlobalCommitter() {
    }

    @Override
    public List<DeltaGlobalCommittable> filterRecoveredCommittables(List<DeltaGlobalCommittable> globalCommittables) {
        return globalCommittables;
    }

    @Override
    public DeltaGlobalCommittable combine(List<DeltaCommittable> committables) {
        return new DeltaGlobalCommittable();
    }


    @Override
    public List<DeltaGlobalCommittable> commit(List<DeltaGlobalCommittable> globalCommittables) {
        return Collections.emptyList();
    }

    @Override
    public void endOfInput() throws IOException {

    }

    @Override
    public void close() throws Exception {

    }


}
