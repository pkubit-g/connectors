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


import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;
import org.apache.flink.connector.delta.sink.writer.DeltaWriter;
import org.apache.flink.connector.delta.sink.writer.DeltaWriterBucketState;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * A unified sink that emits its input elements to {@link FileSystem} files within buckets using
 * Parquet format and commits those files to the DeltaLake table. This
 * sink achieves exactly-once semantics for both {@code BATCH} and {@code STREAMING}.
 * <p>
 * Behaviour of this sink splits down upon two phases. The first phase takes place between application's
 * checkpoints when events are being flushed to file system (or writers' buffers) and the behaviour is almost
 * identical as in case of {@link org.apache.flink.connector.file.sink.FileSink}.
 * Next during the checkpoint phase files are "closed" (renamed) by the independent instances of
 * {@link org.apache.flink.connector.delta.sink.committer.DeltaCommitter} that behave very similar to
 * {@link org.apache.flink.connector.file.sink.committer.FileCommitter}.
 * When all the parallel committers are done, then all the files are committed at once by single-parallelism
 * {@link org.apache.flink.connector.delta.sink.committer.DeltaGlobalCommitter}.
 * <p>
 * This {@link DeltaSink} sources many specific implementations from the {@link org.apache.flink.connector.file.sink.FileSink}
 * so for most of the low level behaviour one may refer to the docs from this module. The most notable differences
 * to the FileSinks are:
 * - tightly coupling DeltaSink to the Bulk-/ParquetFormat
 * - extending committable information with files metadata (name, size, rows)
 * - providing DeltaLake-specific behaviour which is the DeltaGlobalCommitter implementing the commit to the DeltaLog
 * at the final stage of each checkpoint
 *
 * @param <IN> Type of the elements in the input of the sink that are also the elements to be
 *             written to its output
 */
public class DeltaSink<IN> implements Sink<IN, DeltaCommittable, DeltaWriterBucketState, DeltaGlobalCommittable> {

    private final DeltaSinkBuilder<IN> sinkBuilder;

    DeltaSink(DeltaSinkBuilder<IN> bucketsBuilder) {
        this.sinkBuilder = checkNotNull(bucketsBuilder);
    }

    @Override
    public SinkWriter<IN, DeltaCommittable, DeltaWriterBucketState> createWriter(
            InitContext context,
            List<DeltaWriterBucketState> states
    ) throws IOException {
        DeltaWriter<IN> writer = sinkBuilder.createWriter(context);
        writer.initializeState(states);
        return writer;
    }

    @Override
    public Optional<SimpleVersionedSerializer<DeltaWriterBucketState>> getWriterStateSerializer() {
        try {
            return Optional.of(sinkBuilder.getWriterStateSerializer());
        } catch (IOException e) {
            // it's not optimal that we have to do this but creating the serializers for the
            // FileSink/DeltaSink requires (among other things) a call to FileSystem.get() which declares
            // IOException.
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
            // it's not optimal that we have to do this but creating the serializers for the
            // FileSink/DeltaSink requires (among other things) a call to FileSystem.get() which declares
            // IOException.
            throw new FlinkRuntimeException("Could not create committable serializer.", e);
        }
    }

    @Override
    public Optional<GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable>> createGlobalCommitter() throws IOException {

        return Optional.of(sinkBuilder.createGlobalCommitter());
    }

    @Override
    public Optional<SimpleVersionedSerializer<DeltaGlobalCommittable>> getGlobalCommittableSerializer() {
        try {
            return Optional.of(sinkBuilder.getGlobalCommittableSerializer());
        } catch (IOException e) {
            // it's not optimal that we have to do this but creating the serializers for the
            // FileSink requires (among other things) a call to FileSystem.get() which declares
            // IOException.
            throw new FlinkRuntimeException("Could not create committable serializer.", e);
        }
    }


    /**
     * Convenience method where developer must ensure that writerFactory is configured to use SNAPPY
     * compression codec.
     *
     * @param basePath      path of the DeltaLake's table
     * @param conf          Hadoop configuration object that will be used for creating instances of {@link io.delta.standalone.DeltaLog}
     * @param writerFactory writer factory with predefined configuration for creating new writers that will be writing Parquet
     *                      files with DeltaLake's expected format
     * @param <IN>          Type of the elements in the input of the sink that are also the elements to be
     *                      *     written to its output
     * @return builder for the DeltaSink
     */
    public static <IN> DefaultDeltaFormatBuilder<IN> forDeltaFormat(
            final Path basePath,
            final Configuration conf,
            final ParquetWriterFactory<IN> writerFactory
    ) {
        return new DefaultDeltaFormatBuilder<>(
                basePath,
                conf,
                writerFactory,
                new BasePathBucketAssigner<>()
        );
    }


    /**
     * A builder for configuring the sink for bulk-encoding formats, e.g. Parquet.
     */
    public static final class DefaultDeltaFormatBuilder<IN> extends DeltaSinkBuilder<IN> {

        private static final long serialVersionUID = 2818087325120827526L;

        private DefaultDeltaFormatBuilder(
                Path basePath,
                final Configuration conf,
                ParquetWriterFactory<IN> writerFactory,
                BucketAssigner<IN, String> assigner) {
            super(basePath, conf, writerFactory, assigner);
        }
    }

}
