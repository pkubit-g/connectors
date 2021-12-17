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

package org.apache.flink.connector.delta.sink.committables;

import org.apache.flink.annotation.Internal;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * Simple wrapper class required to comply with {@link org.apache.flink.api.connector.sink.GlobalCommitter}
 * interfaces' structure. It's only purpose is to wrap {@link DeltaCommittable} collection during
 * {@link org.apache.flink.connector.delta.sink.committer.DeltaGlobalCommitter#combine} method
 * that will be further flattened and processed inside
 * {@link org.apache.flink.connector.delta.sink.committer.DeltaGlobalCommitter#commit} method.
 */
@Internal
public class DeltaGlobalCommittable {

    private final List<DeltaCommittable> deltaCommittables;

    public DeltaGlobalCommittable(List<DeltaCommittable> deltaCommittables) {
        this.deltaCommittables = checkNotNull(deltaCommittables);
    }

    public List<DeltaCommittable> getDeltaCommittables() {
        return deltaCommittables;
    }

}
