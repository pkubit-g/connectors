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

package org.apache.flink.connector.delta.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.delta.sink.writer.DeltaPendingFile;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import javax.annotation.Nullable;
import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;


@Internal
public class DeltaSinkCommittable implements Serializable {

    @Nullable
    private final DeltaPendingFile deltaPendingFile;


    @Nullable
    private final InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup;


    public DeltaSinkCommittable(DeltaPendingFile deltaPendingFile) {
        this.deltaPendingFile = checkNotNull(deltaPendingFile);
        this.inProgressFileToCleanup = null;
    }

    public DeltaSinkCommittable(
            InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup) {
        this.deltaPendingFile = null;
        this.inProgressFileToCleanup = checkNotNull(inProgressFileToCleanup);
    }

    DeltaSinkCommittable(
            @Nullable DeltaPendingFile deltaPendingFile,
            @Nullable InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup) {
        this.deltaPendingFile = deltaPendingFile;
        this.inProgressFileToCleanup = inProgressFileToCleanup;
    }

    public boolean hasDeltaPendingFile() {
        return deltaPendingFile != null;
    }

    @Nullable
    public DeltaPendingFile getDeltaPendingFile() {
        return deltaPendingFile;
    }

    public boolean hasInProgressFileToCleanup() {
        return inProgressFileToCleanup != null;
    }

    @Nullable
    public InProgressFileWriter.InProgressFileRecoverable getInProgressFileToCleanup() {
        return inProgressFileToCleanup;
    }
}
