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

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;
import org.apache.flink.connector.delta.sink.committer.DeltaGlobalCommitter;
import org.apache.flink.connector.delta.sink.writer.DeltaWriter;
import org.apache.flink.connector.delta.sink.writer.DeltaWriterBucketState;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.hadoop.conf.Configuration;
import static org.apache.flink.util.Preconditions.checkNotNull;

import io.delta.standalone.DeltaLog;

/**
 * A unified sink that emits its input elements to {@link FileSystem} files within buckets using
 * Parquet format and commits those files to the {@link DeltaLog}. This sink achieves exactly-once
 * semantics for both {@code BATCH} and {@code STREAMING}.
 * <p>
 * Behaviour of this sink splits down upon two phases. The first phase takes place between
 * application's checkpoints when records are being flushed to files (or appended to writers'
 * buffers) where the behaviour is almost identical as in case of
 * {@link org.apache.flink.connector.file.sink.FileSink}.
 * <p>
 * Next during the checkpoint phase files are "closed" (renamed) by the independent instances of
 * {@link org.apache.flink.connector.delta.sink.committer.DeltaCommitter} that behave very similar
 * to {@link org.apache.flink.connector.file.sink.committer.FileCommitter}.
 * When all the parallel committers are done, then all the files are committed at once by
 * single-parallelism {@link org.apache.flink.connector.delta.sink.committer.DeltaGlobalCommitter}.
 * <p>
 * This {@link DeltaSink} sources many specific implementations from the
 * {@link org.apache.flink.connector.file.sink.FileSink} so for most of the low level behaviour one
 * may refer to the docs from this module. The most notable differences to the FileSinks are:
 * <ul>
 *  <li>tightly coupling DeltaSink to the Bulk-/ParquetFormat</li>
 *  <li>extending committable information with files metadata (name, size, rows, last update
 *      timestamp)</li>
 *  <li>providing DeltaLake-specific behaviour which is mostly contained in the
 *      {@link DeltaGlobalCommitter} implementing the commit to the {@link DeltaLog} at the final
 *      stage of each checkpoint.</li>
 * </ul>
 * <p>
 * The relations and lifecycle of objects within given {@link DeltaSink} are as follow
 * <ul>
 *     <li>{@link DeltaSink} is the main class exposing the user-facing methods for creating the
 *         sink</li>
 *     <li>{@link DeltaSinkBuilder} is the builder class for {@link DeltaSink} objects. It
 *         exposes all main configuration settings for creating the sink.</li>
 *     <li>{@link DeltaWriter} is the main interface for Flink's {@link Sink} topology performing
 *         the single unit of work on an event-level and generating the committables that will be
 *         used during commit stage. In our case {@link DeltaWriter} delegates the the actual work
 *         to underlying {@link org.apache.flink.connector.delta.sink.writer.DeltaWriterBucket}
 *         objects. The relation between the writer and its buckets is that the writer manages a
 *         collection of the buckets for which it received the events during given checkpoint
 *         interval. Here one bucket writer corresponds to a one partition in a Delta Lake's table.
 *         So in a given checkpoint interval one writer can have zero, one or multiple buckets.</li>
 *     <li>{@link org.apache.flink.connector.delta.sink.writer.DeltaWriterBucket} is being created
 *         always inside {@link DeltaWriter} instance and corresponds to a one partition in Delta
 *         Lake's table (the partition's path and bucket's output path are matching). It is provided
 *         by the {@link DeltaWriter} with incoming stream's events and passes those events
 *         to a particular</li>
 *         {@link org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkPartWriter}
 *         instance that contains underlying logic for flushing data to the OS. Besides that it
 *         manages metadata of the written files and records (both directly and indirectly by using
 *         {@link org.apache.flink.streaming.api.functions.sink.filesystem.DeltaInProgressPart})
 *         and uses those metadata to generate committables' information during a pre-commit
 *         phase.</li>
 * </ul>
 *
 * @param <IN> Type of the elements in the input of the sink that are also the elements to be
 *             written to its output
 * @implNote This sink sources many methods and solutions from
 * {@link org.apache.flink.connector.file.sink.FileSink} implementation simply by copying the
 * code since it was not possible to directly reuse those due to some access specifiers, use of
 * generics and need to provide some internal workarounds compared to the FileSink. To make it
 * explicit which methods are directly copied from FileSink we use `FileSink-specific methods`
 * comment marker inside class files to decouple DeltaLake's specific code from parts borrowed
 * from FileSink.
 */
public class DeltaSink<IN>
    implements Sink<IN, DeltaCommittable, DeltaWriterBucketState, DeltaGlobalCommittable> {

    private final DeltaSinkBuilder<IN> sinkBuilder;

    DeltaSink(DeltaSinkBuilder<IN> sinkBuilder) {
        this.sinkBuilder = checkNotNull(sinkBuilder);
    }

    /**
     * This method creates the {@link SinkWriter} instance that will be responsible for passing
     * incoming stream events to the correct bucket writer and then flushed to the underlying files.
     * <p>
     * The logic for resolving constructor params differ depending on whether any previous writer's
     * states were provided.
     * If there are no previous states then we assume that this is a fresh start of the app and set
     * next checkpoint id in {@link DeltaWriter} to 1 and app id is taken from the
     * {@link DeltaSinkBuilder#getAppId} what guarantees us that each writer will get the same
     * value. In other case, if we are provided by the Flink framework with some previous writers'
     * states then we use those to restore values of appId and nextCheckpointId.
     *
     * @param context {@link SinkWriter} init context object
     * @param states  restored states of the writers. Will be empty collection for fresh start.
     * @return new {@link SinkWriter} object
     * @throws IOException When the recoverable writer cannot be instantiated.
     */
    @Override
    public SinkWriter<IN, DeltaCommittable, DeltaWriterBucketState> createWriter(
        InitContext context,
        List<DeltaWriterBucketState> states
    ) throws IOException {
        String appId = restoreOrCreateAppId(states);
        long nextCheckpointId = restoreOrGetNextCheckpointId(states);
        DeltaWriter<IN> writer = sinkBuilder.createWriter(context, appId, nextCheckpointId);
        writer.initializeState(states);
        return writer;
    }

    /**
     * Restores application's id snapshotted in any of the {@link DeltaWriter}s' states or gets
     * new one from the builder in case there is no previous states.
     * <p>
     * In order to gurantee the idempotency of the GlobalCommitter we need unique identifier of the
     * app. We obtain it with simple logic: if it's the first run of the application (so no restart
     * from snapshot or failure recovery happened and the writer's state is empty) then assign appId
     * to a newly generated UUID that will be further stored in the state of each writer.
     * Alternatively if the writer's states are not empty then we resolve appId from one of the
     * restored states.
     *
     * @param states restored list of writer's buckets states that include previously generated
     *               appId
     * @return newly created or resolved from restored writer's states unique identifier of the app.
     */
    private String restoreOrCreateAppId(List<DeltaWriterBucketState> states) {
        if (states.isEmpty()) {
            return sinkBuilder.getAppId();
        }
        return states.get(0).getAppId();
    }

    /**
     * Restores the last checkpoint id snapshotted in one of the most recent {@link DeltaWriter}s'
     * states or sets it to "1" in case there is no previous states.
     * <p>
     * In order to gurantee the idempotency of the GlobalCommitter we need to version consecutive
     * commits with consecutive identifiers. For this purpose we are using checkpointId that is
     * being "manually" managed in writer's internal logic, added to the committables information
     * and incremented on every precommit action (after generating the committables).
     * <p>
     * To restore the last recent checkpoint id we need to get max value of this param within all
     * the given states. We are interested in max because at this stage if we are given states with
     * checkpoint ids lower than the max then it means that committables for those checkpoint
     * intervals have been already generated and checkpointed in the job's state so when creating
     * new writers we should pass them the most recent value of resolved checkpoint id (the writers
     * will use this value for generating new set of committables for next incoming checkpoint
     * interval).
     *
     * @param states restored list of writer's buckets states that include previously generated
     *               appId
     * @return value of the nextCheckpointId to be passed to the new writer's instance
     */
    private long restoreOrGetNextCheckpointId(List<DeltaWriterBucketState> states) {
        if (states.isEmpty()) {
            return 1;
        }
        return states
            .stream()
            .map(DeltaWriterBucketState::getCheckpointId)
            .mapToLong(v -> v)
            .max()
            .getAsLong();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DeltaWriterBucketState>> getWriterStateSerializer() {
        try {
            return Optional.of(sinkBuilder.getWriterStateSerializer());
        } catch (IOException e) {
            throw new FlinkRuntimeException("Could not create writer state serializer.", e);
        }
    }

    @Override
    public Optional<Committer<DeltaCommittable>> createCommitter() throws IOException {
        return Optional.of(sinkBuilder.createCommitter());
    }

    @Override
    public Optional<SimpleVersionedSerializer<DeltaCommittable>> getCommittableSerializer() {
        try {
            return Optional.of(sinkBuilder.getCommittableSerializer());
        } catch (IOException e) {
            throw new FlinkRuntimeException("Could not create committable serializer.", e);
        }
    }

    @Override
    public Optional<GlobalCommitter<DeltaCommittable,
        DeltaGlobalCommittable>> createGlobalCommitter() {
        return Optional.of(sinkBuilder.createGlobalCommitter());
    }

    @Override
    public Optional<
        SimpleVersionedSerializer<DeltaGlobalCommittable>> getGlobalCommittableSerializer() {
        try {
            return Optional.of(sinkBuilder.getGlobalCommittableSerializer());
        } catch (IOException e) {
            throw new FlinkRuntimeException("Could not create committable serializer.", e);
        }
    }

    /**
     * Convenience method for creating {@link DeltaSink} to a non-partitioned DeltaLake's table.
     *
     * @param basePath root path of the DeltaLake's table
     * @param conf     Hadoop's conf object that will be used for creating instances of
     *                 {@link io.delta.standalone.DeltaLog} and will be also passed to the
     *                 {@link ParquetRowDataBuilder} to create {@link ParquetWriterFactory}
     * @param rowType  Flink's logical type to indicate the structure of the events in the stream
     * @return builder for the DeltaSink
     */
    public static DeltaSinkBuilder<RowData> forDeltaFormat(
        final Path basePath,
        final Configuration conf,
        final RowType rowType
    ) {
        return forDeltaFormat(
            basePath, conf, rowType, new BasePathBucketAssigner<>()
        );
    }

    /**
     * Convenience method for creating {@link DeltaSink} to a partitioned DeltaLake's table.
     *
     * @param basePath root path of the DeltaLake's table
     * @param conf     Hadoop's conf object that will be used for creating instances of
     *                 {@link io.delta.standalone.DeltaLog} and will be also passed to the
     *                 {@link ParquetRowDataBuilder} to create {@link ParquetWriterFactory}
     * @param rowType  Flink's logical type to indicate the structure of the events in the stream
     * @param assigner {@link BucketAssigner} object containing the partition assignment behaviour.
     *                 It is advised to use {@link DeltaTablePartitionAssigner} for most of the
     *                 cases, however for advanced partition assignment behaviour users can provide
     *                 their own implementation of {@link BucketAssigner}.
     * @return builder for the DeltaSink
     */
    public static DeltaSinkBuilder<RowData> forDeltaFormat(
        final Path basePath,
        final Configuration conf,
        final RowType rowType,
        BucketAssigner<RowData, String> assigner
    ) {
        conf.set("parquet.compression", "SNAPPY");
        ParquetWriterFactory<RowData> writerFactory = ParquetRowDataBuilder.createWriterFactory(
            rowType,
            conf,
            true // utcTimestamp
        );

        return new DeltaSinkBuilder.DefaultDeltaFormatBuilder<>(
            basePath,
            conf,
            writerFactory,
            assigner,
            OnCheckpointRollingPolicy.build(),
            rowType,
            false // canTryUpdateSchema
        );
    }
}
