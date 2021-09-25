package org.apache.flink.connector.delta.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import javax.annotation.Nullable;

@Internal
public class DeltaWriterBucketState {

    private final String bucketId;

    /**
     * The directory where all the part files of the bucket are stored.
     */
    private final Path bucketPath;

    /**
     * The creation time of the currently open part file, or {@code Long.MAX_VALUE} if there is no
     * open part file.
     */
    private final long inProgressFileCreationTime;

    /**
     * A {@link InProgressFileWriter.InProgressFileRecoverable} for the currently open part file, or null if there is no
     * currently open part file.
     */
    @Nullable
    private final InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable;

    @Nullable
    private final Path inProgressPartFilePath;

    public DeltaWriterBucketState(
            String bucketId,
            Path bucketPath,
            long inProgressFileCreationTime,
            @Nullable InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable,
            @Nullable Path inProgressPartFilePath) {
        this.bucketId = bucketId;
        this.bucketPath = bucketPath;
        this.inProgressFileCreationTime = inProgressFileCreationTime;
        this.inProgressFileRecoverable = inProgressFileRecoverable;
        this.inProgressPartFilePath = inProgressPartFilePath;
    }

    public String getBucketId() {
        return bucketId;
    }

    public Path getBucketPath() {
        return bucketPath;
    }

    public long getInProgressFileCreationTime() {
        return inProgressFileCreationTime;
    }

    @Nullable
    InProgressFileWriter.InProgressFileRecoverable getInProgressFileRecoverable() {
        return inProgressFileRecoverable;
    }

    @Nullable
    Path getInProgressPartFilePath() {
        return inProgressPartFilePath;
    }


    boolean hasInProgressFileRecoverable() {
        return inProgressFileRecoverable != null;
    }


    @Override
    public String toString() {
        final StringBuilder strBuilder = new StringBuilder();

        strBuilder
                .append("BucketState for bucketId=")
                .append(bucketId)
                .append(" and bucketPath=")
                .append(bucketPath);

        if (hasInProgressFileRecoverable()) {
            strBuilder.append(", has open part file created @ ").append(inProgressFileCreationTime);
        }

        return strBuilder.toString();
    }

}
