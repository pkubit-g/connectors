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

import io.delta.standalone.DeltaLog;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class DeltaGlobalCommitter implements GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable> {

    /**
     * Hadoop configuration that is passed to {@link DeltaLog} instance when creating it
     */
    private final Configuration conf;

    /**
     * Root path of the DeltaTable
     */
    private final Path basePath;

    public DeltaGlobalCommitter(Configuration conf,
                                Path basePath) {
        this.conf = conf;
        this.basePath = basePath;
    }

    @Override
    public List<DeltaGlobalCommittable> filterRecoveredCommittables(List<DeltaGlobalCommittable> globalCommittables) {
        return globalCommittables;
    }

    @Override
    public DeltaGlobalCommittable combine(List<DeltaCommittable> committables) {
        return new DeltaGlobalCommittable(committables);
    }

    /**
     * Resolves appId param from the first committable object. It does not matter which object as all committables carry the same appId value.
     * It's ok to return null value here as it would mean that there are no committables (aka no stream events were received) for given checkpoint.
     *
     * @param globalCommittables list of global committables objects
     * @return unique app identifier for given Flink job
     */
    @Nullable
    private String resolveAppId(List<DeltaGlobalCommittable> globalCommittables) {
        for (DeltaGlobalCommittable globalCommittable : globalCommittables) {
            for (DeltaCommittable deltaCommittable : globalCommittable.getDeltaCommittables()) {
                return deltaCommittable.getAppId();
            }
        }
        return null;
    }

    @Override
    public List<DeltaGlobalCommittable> commit(List<DeltaGlobalCommittable> globalCommittables) {
        String appId = resolveAppId(globalCommittables);
        // some commit logic
        return Collections.emptyList();
    }

    @Override
    public void endOfInput() {
    }

    @Override
    public void close() {
    }

}
