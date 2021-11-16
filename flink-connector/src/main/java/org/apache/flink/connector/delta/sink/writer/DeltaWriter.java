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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.file.sink.writer.FileWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link SinkWriter} implementation for {@link org.apache.flink.connector.delta.sink.DeltaSink}.
 *
 * <p>
 * It writes data to and manages the different active {@link DeltaWriterBucket buckets} in the
 * {@link org.apache.flink.connector.delta.sink.DeltaSink}.
 * <p>
 * Most of the logic for this class was sourced from {@link FileWriter} as the behaviour is very
 * similar. The main differences are use of custom implementations for some member classes and also
 * managing {@link io.delta.standalone.DeltaLog} transactional ids.
 *
 * @param <IN> The type of input elements.
 */
@Internal
public class DeltaWriter<IN>
    implements SinkWriter<IN, DeltaCommittable, DeltaWriterBucketState>,
    Sink.ProcessingTimeService.ProcessingTimeCallback {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaWriter.class);

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific fields
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////
    // configuration fields
    ///////////////////////////////////////////////////

    private final DeltaBulkBucketWriter<IN, String> bucketWriter;

    private final CheckpointRollingPolicy<IN, String> rollingPolicy;

    private final Path basePath;

    private final BucketAssigner<IN, String> bucketAssigner;

    private final Sink.ProcessingTimeService processingTimeService;

    private final long bucketCheckInterval;

    ///////////////////////////////////////////////////
    // runtime fields
    ///////////////////////////////////////////////////

    private final Map<String, DeltaWriterBucket<IN>> activeBuckets;

    private final BucketerContext bucketerContext;

    private final OutputFileConfig outputFileConfig;

    /**
     * A constructor creating a new empty bucket (DeltaLake table's partitions) manager.
     *
     * @param basePath       the base path for the table
     * @param bucketAssigner The {@link BucketAssigner} provided by the user.
     * @param bucketWriter   The {@link DeltaBulkBucketWriter} to be used when writing data.
     * @param rollingPolicy  The {@link CheckpointRollingPolicy} as specified by the user.
     */
    public DeltaWriter(
        final Path basePath,
        final BucketAssigner<IN, String> bucketAssigner,
        final DeltaBulkBucketWriter<IN, String> bucketWriter,
        final CheckpointRollingPolicy<IN, String> rollingPolicy,
        final OutputFileConfig outputFileConfig,
        final Sink.ProcessingTimeService processingTimeService,
        final long bucketCheckInterval) {

        this.basePath = checkNotNull(basePath);
        this.bucketAssigner = checkNotNull(bucketAssigner);
        this.bucketWriter = checkNotNull(bucketWriter);
        this.rollingPolicy = checkNotNull(rollingPolicy);

        this.outputFileConfig = checkNotNull(outputFileConfig);

        this.activeBuckets = new HashMap<>();
        this.bucketerContext = new BucketerContext();

        this.processingTimeService = checkNotNull(processingTimeService);
        checkArgument(
            bucketCheckInterval > 0,
            "Bucket checking interval for processing time should be positive.");
        this.bucketCheckInterval = bucketCheckInterval;
    }

    /**
     * @implNote This method behaves in the similar way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter#snapshotState}
     * except that it uses custom {@link DeltaWriterBucketState} and {@link DeltaWriterBucket}
     * implementations. Custom implementation are needed in order to extend the committables'
     * information with metadata of written files and also to customize the state that is being
     * snapshotted during checkpoint phase.
     * <p>
     * Additionally, it implements snapshotting writer's states even in case when there are no
     * active buckets (which may be not such a rare case e.g. when checkpoint interval will be very
     * short and the writer will not receive any data during this interval then it will mark the
     * buckets as inactive). This behaviour is needed for delta-specific case when we want to retain
     * the same application id within all app restarts / recreation writers' states from snapshot.
     */
    @Override
    public List<DeltaWriterBucketState> snapshotState() {
        checkState(bucketWriter != null, "sink has not been initialized");

        List<DeltaWriterBucketState> states = new ArrayList<>();
        for (DeltaWriterBucket<IN> bucket : activeBuckets.values()) {
            states.add(bucket.snapshotState());
        }

        return states;
    }

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * A proxy method that forwards the incoming event to the correct {@link DeltaWriterBucket}
     * instance.
     *
     * @param element incoming stream event
     * @param context context for getting additional data about input event
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter#write}
     * except that it uses custom {@link DeltaWriterBucket} implementation.
     */
    @Override
    public void write(IN element, Context context) throws IOException {
        bucketerContext.update(
            context.timestamp(),
            context.currentWatermark(),
            processingTimeService.getCurrentProcessingTime());

        final String bucketId = bucketAssigner.getBucketId(element, bucketerContext);
        final DeltaWriterBucket<IN> bucket = getOrCreateBucketForBucketId(bucketId);
        bucket.write(element, processingTimeService.getCurrentProcessingTime());
    }

    /**
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter#prepareCommit}
     * except that it uses custom {@link DeltaWriterBucket} implementation.
     */
    @Override
    public List<DeltaCommittable> prepareCommit(boolean flush) throws IOException {
        List<DeltaCommittable> committables = new ArrayList<>();

        // Every time before we prepare commit, we first check and remove the inactive
        // buckets. Checking the activeness right before pre-committing avoid re-creating
        // the bucket every time if the bucket use OnCheckpointingRollingPolicy.
        Iterator<Map.Entry<String, DeltaWriterBucket<IN>>> activeBucketIt =
            activeBuckets.entrySet().iterator();
        while (activeBucketIt.hasNext()) {
            Map.Entry<String, DeltaWriterBucket<IN>> entry = activeBucketIt.next();
            if (!entry.getValue().isActive()) {
                activeBucketIt.remove();
            } else {
                committables.addAll(entry.getValue().prepareCommit(flush));
            }
        }

        return committables;
    }

    /**
     * Initializes the state from snapshoted {@link DeltaWriterBucketState}.
     *
     * @param bucketStates the state holding recovered state about active buckets.
     * @throws IOException if anything goes wrong during retrieving the state or
     *                     restoring/committing of any in-progress/pending part files
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter#initializeState}
     * except that it uses custom {@link DeltaWriterBucketState} and {@link DeltaWriterBucket}
     * implementations.
     */
    public void initializeState(List<DeltaWriterBucketState> bucketStates) throws IOException {
        checkNotNull(bucketStates, "The retrieved state was null.");

        for (DeltaWriterBucketState state : bucketStates) {
            String bucketId = state.getBucketId();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Restoring: {}", state);
            }

            DeltaWriterBucket<IN> restoredBucket =
                DeltaWriterBucket.DeltaWriterBucketFactory.restoreBucket(
                    bucketWriter, rollingPolicy, state, outputFileConfig);

            updateActiveBucketId(bucketId, restoredBucket);
        }

        registerNextBucketInspectionTimer();
    }

    /**
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter}#updateActiveBucketId
     * except that it uses custom {@link DeltaWriterBucket} implementation.
     */
    private void updateActiveBucketId(String bucketId,
                                      DeltaWriterBucket<IN> restoredBucket)
        throws IOException {
        final DeltaWriterBucket<IN> bucket = activeBuckets.get(bucketId);
        if (bucket != null) {
            bucket.merge(restoredBucket);
        } else {
            activeBuckets.put(bucketId, restoredBucket);
        }
    }

    /**
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter}#getOrCreateBucketForBucketId
     * except that it uses custom {@link DeltaWriterBucket} implementation.
     */
    private DeltaWriterBucket<IN> getOrCreateBucketForBucketId(String bucketId) {
        DeltaWriterBucket<IN> bucket = activeBuckets.get(bucketId);
        if (bucket == null) {
            final Path bucketPath = assembleBucketPath(bucketId);
            bucket =
                DeltaWriterBucket.DeltaWriterBucketFactory.getNewBucket(
                    bucketId, bucketPath, bucketWriter, rollingPolicy, outputFileConfig);
            activeBuckets.put(bucketId, bucket);
        }
        return bucket;
    }

    /**
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter#close}
     * except that it uses custom {@link DeltaWriterBucket} implementation.
     */
    @Override
    public void close() {
        if (activeBuckets != null) {
            activeBuckets.values().forEach(DeltaWriterBucket::disposePartFile);
        }
    }

    /**
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter}#assembleBucketPath.
     */
    private Path assembleBucketPath(String bucketId) {
        if ("".equals(bucketId)) {
            return basePath;
        }
        return new Path(basePath, bucketId);
    }

    /**
     * @implNote This method behaves in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter#onProcessingTime}
     * except that it uses custom {@link DeltaWriterBucket} implementation.
     */
    @Override
    public void onProcessingTime(long time) throws IOException {
        for (DeltaWriterBucket<IN> bucket : activeBuckets.values()) {
            bucket.onProcessingTime(time);
        }

        registerNextBucketInspectionTimer();
    }

    /**
     * @implNote This method behaves in the same way as in
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter}
     */
    private void registerNextBucketInspectionTimer() {
        final long nextInspectionTime =
            processingTimeService.getCurrentProcessingTime() + bucketCheckInterval;
        processingTimeService.registerProcessingTimer(nextInspectionTime, this);
    }

    /**
     * The {@link BucketAssigner.Context} exposed to the {@link BucketAssigner#getBucketId(Object,
     * BucketAssigner.Context)} whenever a new incoming element arrives.
     *
     * @implNote This class is implemented in the same way as
     * {@link org.apache.flink.connector.file.sink.writer.FileWriter}.BucketerContext.
     */
    private static final class BucketerContext implements BucketAssigner.Context {

        @Nullable
        private Long elementTimestamp;

        private long currentWatermark;

        private long currentProcessingTime;

        private BucketerContext() {
            this.elementTimestamp = null;
            this.currentWatermark = Long.MIN_VALUE;
            this.currentProcessingTime = Long.MIN_VALUE;
        }

        void update(@Nullable Long elementTimestamp, long watermark, long currentProcessingTime) {
            this.elementTimestamp = elementTimestamp;
            this.currentWatermark = watermark;
            this.currentProcessingTime = currentProcessingTime;
        }

        @Override
        public long currentProcessingTime() {
            return currentProcessingTime;
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        @Nullable
        public Long timestamp() {
            return elementTimestamp;
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Testing Methods
    ///////////////////////////////////////////////////////////////////////////

    @VisibleForTesting
    Map<String, DeltaWriterBucket<IN>> getActiveBuckets() {
        return activeBuckets;
    }
}
