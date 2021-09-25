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
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.delta.sink.commiter.DeltaCommitter;
import org.apache.flink.connector.delta.sink.writer.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.BulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;


public class DeltaSink<IN> implements Sink<IN, DeltaSinkCommittable, DeltaWriterBucketState, Void> {

    private final BucketsBuilder<IN, ? extends BucketsBuilder<IN, ?>> bucketsBuilder;

    private DeltaSink(BucketsBuilder<IN, ? extends BucketsBuilder<IN, ?>> bucketsBuilder) {
        this.bucketsBuilder = checkNotNull(bucketsBuilder);
    }

    @Override
    public SinkWriter<IN, DeltaSinkCommittable, DeltaWriterBucketState> createWriter(
            InitContext context,
            List<DeltaWriterBucketState> states
    ) throws IOException {
        DeltaWriter<IN> writer = bucketsBuilder.createWriter(context);
        writer.initializeState(states);
        return writer;
    }

    @Override
    public Optional<SimpleVersionedSerializer<DeltaWriterBucketState>> getWriterStateSerializer() {
        try {
            return Optional.of(bucketsBuilder.getWriterStateSerializer());
        } catch (IOException e) {
            // it's not optimal that we have to do this but creating the serializers for the
            // FileSink requires (among other things) a call to FileSystem.get() which declares
            // IOException.
            throw new FlinkRuntimeException("Could not create writer state serializer.", e);
        }
    }

    @Override
    public Optional<Committer<DeltaSinkCommittable>> createCommitter() throws IOException {
        return Optional.of(bucketsBuilder.createCommitter());
    }

    @Override
    public Optional<SimpleVersionedSerializer<DeltaSinkCommittable>> getCommittableSerializer() {
        try {
            return Optional.of(bucketsBuilder.getCommittableSerializer());
        } catch (IOException e) {
            // it's not optimal that we have to do this but creating the serializers for the
            // FileSink requires (among other things) a call to FileSystem.get() which declares
            // IOException.
            throw new FlinkRuntimeException("Could not create committable serializer.", e);
        }
    }

    @Override
    public Optional<GlobalCommitter<DeltaSinkCommittable, Void>> createGlobalCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }


    public static <IN> DefaultBulkFormatBuilder<IN> forBulkFormat(
            final Path basePath, final BulkWriter.Factory<IN> bulkWriterFactory) {
        return new DefaultBulkFormatBuilder<>(
                basePath, bulkWriterFactory, new DateTimeBucketAssigner<>());
    }

    /**
     * The base abstract class for the {@link BulkFormatBuilder}.
     */
    @Internal
    private abstract static class BucketsBuilder<IN, T extends BucketsBuilder<IN, T>>
            implements Serializable {

        private static final long serialVersionUID = 1L;

        protected static final long DEFAULT_BUCKET_CHECK_INTERVAL = 60L * 1000L;

        @SuppressWarnings("unchecked")
        protected T self() {
            return (T) this;
        }

        @Internal
        abstract DeltaWriter<IN> createWriter(final InitContext context) throws IOException;

        @Internal
        abstract DeltaCommitter createCommitter() throws IOException;

        @Internal
        abstract SimpleVersionedSerializer<DeltaWriterBucketState> getWriterStateSerializer()
                throws IOException;

        @Internal
        abstract SimpleVersionedSerializer<DeltaSinkCommittable> getCommittableSerializer()
                throws IOException;
    }


    /**
     * A builder for configuring the sink for bulk-encoding formats, e.g. Parquet/ORC.
     */
    @PublicEvolving
    public static class BulkFormatBuilder<IN, T extends BulkFormatBuilder<IN, T>>
            extends BucketsBuilder<IN, T> {

        private static final long serialVersionUID = 1L;

        private final Path basePath;

        private long bucketCheckInterval;

        private final BulkWriter.Factory<IN> writerFactory;

        private final DeltaWriterBucketFactory<IN> bucketFactory;

        private BucketAssigner<IN, String> bucketAssigner;

        private CheckpointRollingPolicy<IN, String> rollingPolicy;

        private OutputFileConfig outputFileConfig;

        protected BulkFormatBuilder(
                Path basePath,
                BulkWriter.Factory<IN> writerFactory,
                BucketAssigner<IN, String> assigner) {
            this(
                    basePath,
                    DEFAULT_BUCKET_CHECK_INTERVAL,
                    writerFactory,
                    assigner,
                    OnCheckpointRollingPolicy.build(),
                    new DefaultDeltaWriterBucketFactory<>(),
                    OutputFileConfig.builder().build());
        }

        protected BulkFormatBuilder(
                Path basePath,
                long bucketCheckInterval,
                BulkWriter.Factory<IN> writerFactory,
                BucketAssigner<IN, String> assigner,
                CheckpointRollingPolicy<IN, String> policy,
                DeltaWriterBucketFactory<IN> bucketFactory,
                OutputFileConfig outputFileConfig) {
            this.basePath = checkNotNull(basePath);
            this.bucketCheckInterval = bucketCheckInterval;
            this.writerFactory = writerFactory;
            this.bucketAssigner = checkNotNull(assigner);
            this.rollingPolicy = checkNotNull(policy);
            this.bucketFactory = checkNotNull(bucketFactory);
            this.outputFileConfig = checkNotNull(outputFileConfig);
        }

        public T withBucketCheckInterval(final long interval) {
            this.bucketCheckInterval = interval;
            return self();
        }

        public T withBucketAssigner(BucketAssigner<IN, String> assigner) {
            this.bucketAssigner = checkNotNull(assigner);
            return self();
        }

        public T withRollingPolicy(CheckpointRollingPolicy<IN, String> rollingPolicy) {
            this.rollingPolicy = checkNotNull(rollingPolicy);
            return self();
        }

        public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
            this.outputFileConfig = outputFileConfig;
            return self();
        }

        public BulkFormatBuilder<IN, ? extends BulkFormatBuilder<IN, ?>> withNewBucketAssigner(
                BucketAssigner<IN, String> assigner) {
            checkState(
                    bucketFactory.getClass() == DefaultDeltaWriterBucketFactory.class,
                    "newBuilderWithBucketAssigner() cannot be called "
                            + "after specifying a customized bucket factory");
            return new BulkFormatBuilder<>(
                    basePath,
                    bucketCheckInterval,
                    writerFactory,
                    checkNotNull(assigner),
                    rollingPolicy,
                    bucketFactory,
                    outputFileConfig);
        }

        /**
         * Creates the actual sink.
         */
        public DeltaSink<IN> build() {
            return new DeltaSink<>(this);
        }

        @Override
        DeltaWriter<IN> createWriter(InitContext context) throws IOException {
            return new DeltaWriter<IN>(
                    basePath,
                    bucketAssigner,
                    bucketFactory,
                    createBucketWriter(),
                    rollingPolicy,
                    outputFileConfig,
                    context.getProcessingTimeService(),
                    bucketCheckInterval);
        }

        @Override
        DeltaCommitter createCommitter() throws IOException {
            return new DeltaCommitter(createBucketWriter());
        }

        @Override
        SimpleVersionedSerializer<DeltaWriterBucketState> getWriterStateSerializer()
                throws IOException {
            BucketWriter<IN, String> bucketWriter = createBucketWriter();

            return new DeltaWriterBucketStateSerializer(
                    bucketWriter.getProperties().getInProgressFileRecoverableSerializer()
            );
        }

        @Override
        SimpleVersionedSerializer<DeltaSinkCommittable> getCommittableSerializer()
                throws IOException {
            BucketWriter<IN, String> bucketWriter = createBucketWriter();

            return new DeltaSinkCommittableSerializer(
                    bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                    bucketWriter.getProperties().getInProgressFileRecoverableSerializer());
        }

        private BucketWriter<IN, String> createBucketWriter() throws IOException {
            return new BulkBucketWriter<>(
                    FileSystem.get(basePath.toUri()).createRecoverableWriter(), writerFactory);
        }
    }

    /**
     * Builder for the vanilla {@link DeltaSink} using a bulk format.
     *
     * @param <IN> record type
     */
    public static final class DefaultBulkFormatBuilder<IN>
            extends BulkFormatBuilder<IN, DefaultBulkFormatBuilder<IN>> {

        private static final long serialVersionUID = 7493169281036370228L;

        private DefaultBulkFormatBuilder(
                Path basePath,
                BulkWriter.Factory<IN> writerFactory,
                BucketAssigner<IN, String> assigner) {
            super(basePath, writerFactory, assigner);
        }
    }
}
