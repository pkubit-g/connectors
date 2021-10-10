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


import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.delta.sink.commiter.DeltaCommitter;
import org.apache.flink.connector.delta.sink.commiter.DeltaGlobalCommitter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittableSerializer;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittableSerializer;
import org.apache.flink.connector.delta.sink.writer.DefaultDeltaWriterBucketFactory;
import org.apache.flink.connector.delta.sink.writer.DeltaWriter;
import org.apache.flink.connector.delta.sink.writer.DeltaWriterBucketFactory;
import org.apache.flink.connector.delta.sink.writer.DeltaWriterBucketState;
import org.apache.flink.connector.delta.sink.writer.DeltaWriterBucketStateSerializer;
//import org.apache.flink.connector.delta.sink.writer.parquet.DeltaParquetWriterBuilder;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;


public class DeltaSink<IN> implements Sink<IN, DeltaCommittable, DeltaWriterBucketState, DeltaGlobalCommittable> {

    private final DeltaFormatBuilder<IN, ? extends DeltaFormatBuilder<IN, ?>> bucketsBuilder;

    private DeltaSink(DeltaFormatBuilder<IN, ? extends DeltaFormatBuilder<IN, ?>> bucketsBuilder) {
        this.bucketsBuilder = checkNotNull(bucketsBuilder);
    }

    @Override
    public SinkWriter<IN, DeltaCommittable, DeltaWriterBucketState> createWriter(
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
    public Optional<Committer<DeltaCommittable>> createCommitter() throws IOException {
        return Optional.of(bucketsBuilder.createCommitter());
    }

    @Override
    public Optional<SimpleVersionedSerializer<DeltaCommittable>> getCommittableSerializer() {
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
    public Optional<GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable>> createGlobalCommitter() throws IOException {

        return Optional.of(bucketsBuilder.createGlobalCommitter());
    }

    @Override
    public Optional<SimpleVersionedSerializer<DeltaGlobalCommittable>> getGlobalCommittableSerializer() {
        try {
            return Optional.of(bucketsBuilder.getGlobalCommittableSerializer());
        } catch (IOException e) {
            // it's not optimal that we have to do this but creating the serializers for the
            // FileSink requires (among other things) a call to FileSystem.get() which declares
            // IOException.
            throw new FlinkRuntimeException("Could not create committable serializer.", e);
        }
    }


    /**
     * Convenience method that will create builder configured to use SNAPPY compression codec.
     *
     * @param basePath
     * @param conf
     * @param writeSupport
     * @param <IN>
     * @return
     */
//    public static <IN> DefaultDeltaFormatBuilder<IN> forDeltaFormat(
//            final Path basePath,
//            final Configuration conf,
//            WriteSupport<IN> writeSupport
//    ) {
//        return new DefaultDeltaFormatBuilder<>(
//                basePath,
//                conf,
//                DeltaParquetWriterBuilder.createWriterFactory(conf, writeSupport),
//                new BasePathBucketAssigner<>()
//        );
//    }

    /**
     * Convenience method where developer must ensure that bulkWriterFactory is configured to use SNAPPY
     * compression codec.
     *
     * @param basePath
     * @param conf
     * @param bulkWriterFactory
     * @param <IN>
     * @return
     */
    public static <IN> DefaultDeltaFormatBuilder<IN> forDeltaFormat(
            final Path basePath,
            final Configuration conf,
            final ParquetWriterFactory<IN> bulkWriterFactory
    ) {
        return new DefaultDeltaFormatBuilder<>(
                basePath,
                conf,
                bulkWriterFactory,
                new BasePathBucketAssigner<>()
        );
    }


    /**
     * A builder for configuring the sink for bulk-encoding formats, e.g. Parquet/ORC.
     */

    public static final class DefaultDeltaFormatBuilder<IN>
            extends DeltaFormatBuilder<IN, DefaultDeltaFormatBuilder<IN>> {

        private static final long serialVersionUID = 7493169281036370228L;

        private DefaultDeltaFormatBuilder(
                Path basePath,
                final Configuration conf,
                ParquetWriterFactory<IN> writerFactory,
                BucketAssigner<IN, String> assigner) {
            super(basePath, conf, writerFactory, assigner);
        }
    }

    @PublicEvolving
    public static class DeltaFormatBuilder<IN, T extends DeltaFormatBuilder<IN, T>>
            implements Serializable {

        private static final long serialVersionUID = 7493169281036370228L;

        protected static final long DEFAULT_BUCKET_CHECK_INTERVAL = 60L * 1000L;

        private final Path basePath;

        private long bucketCheckInterval;

        private final ParquetWriterFactory<IN> writerFactory;

        private final DeltaWriterBucketFactory<IN> bucketFactory;

        private BucketAssigner<IN, String> bucketAssigner;

        private CheckpointRollingPolicy<IN, String> rollingPolicy;

        private OutputFileConfig outputFileConfig;

        private final SerializableConfiguration configuration;

        @SuppressWarnings("unchecked")
        protected T self() {
            return (T) this;
        }

        protected DeltaFormatBuilder(
                Path basePath,
                Configuration conf,
                ParquetWriterFactory<IN> writerFactory,
                BucketAssigner<IN, String> assigner) {
            this(
                    basePath,
                    conf,
                    DEFAULT_BUCKET_CHECK_INTERVAL,
                    writerFactory,
                    assigner,
                    OnCheckpointRollingPolicy.build(),
                    new DefaultDeltaWriterBucketFactory<>(),
                    OutputFileConfig.builder().withPartSuffix(".snappy.parquet").build());
        }

        protected DeltaFormatBuilder(
                Path basePath,
                Configuration conf,
                long bucketCheckInterval,
                ParquetWriterFactory<IN> writerFactory,
                BucketAssigner<IN, String> assigner,
                CheckpointRollingPolicy<IN, String> policy,
                DeltaWriterBucketFactory<IN> bucketFactory,
                OutputFileConfig outputFileConfig) {
            this.basePath = checkNotNull(basePath);
            this.configuration = new SerializableConfiguration(checkNotNull(conf));
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

        public DeltaFormatBuilder<IN, ? extends DeltaFormatBuilder<IN, ?>> withNewBucketAssigner(
                BucketAssigner<IN, String> assigner) {
            checkState(
                    bucketFactory.getClass() == DefaultDeltaWriterBucketFactory.class,
                    "newBuilderWithBucketAssigner() cannot be called "
                            + "after specifying a customized bucket factory");
            return new DeltaFormatBuilder<>(
                    basePath,
                    configuration.conf(),
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

        DeltaCommitter createCommitter() throws IOException {
            return new DeltaCommitter(createBucketWriter());
        }

        DeltaGlobalCommitter createGlobalCommitter() throws IOException {
            return new DeltaGlobalCommitter(configuration.conf(), basePath);
        }

        SimpleVersionedSerializer<DeltaWriterBucketState> getWriterStateSerializer()
                throws IOException {
            BucketWriter<IN, String> bucketWriter = createBucketWriter();

            return new DeltaWriterBucketStateSerializer(
                    bucketWriter.getProperties().getInProgressFileRecoverableSerializer()
            );
        }

        SimpleVersionedSerializer<DeltaCommittable> getCommittableSerializer()
                throws IOException {
            BucketWriter<IN, String> bucketWriter = createBucketWriter();

            return new DeltaCommittableSerializer(
                    bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                    bucketWriter.getProperties().getInProgressFileRecoverableSerializer());
        }

        SimpleVersionedSerializer<DeltaGlobalCommittable> getGlobalCommittableSerializer()
                throws IOException {
            BucketWriter<IN, String> bucketWriter = createBucketWriter();

            return new DeltaGlobalCommittableSerializer(bucketWriter.getProperties().getPendingFileRecoverableSerializer());
        }

        private DeltaBulkBucketWriter<IN, String> createBucketWriter() throws IOException {
            return new DeltaBulkBucketWriter<>(
                    FileSystem.get(basePath.toUri()).createRecoverableWriter(), writerFactory);
        }
    }

}
