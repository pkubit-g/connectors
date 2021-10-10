package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

public class DeltaBulkBucketWriter<IN, BucketID> extends BulkBucketWriter<IN, BucketID> {

    private final BulkWriter.Factory<IN> writerFactory;

    public DeltaBulkBucketWriter(final RecoverableWriter recoverableWriter,
                                 BulkWriter.Factory<IN> writerFactory)
            throws IOException {
        super(recoverableWriter, writerFactory);
        this.writerFactory = writerFactory;
    }

    @Override
    public DeltaBulkPartWriter<IN, BucketID> resumeFrom(
            final BucketID bucketId,
            final RecoverableFsDataOutputStream stream,
            final RecoverableWriter.ResumeRecoverable resumable,
            final long creationTime)
            throws IOException {
        Preconditions.checkNotNull(stream);
        Preconditions.checkNotNull(resumable);

        final BulkWriter<IN> writer = writerFactory.create(stream);
        return new DeltaBulkPartWriter<>(bucketId, stream, writer, creationTime);
    }

    @Override
    public DeltaBulkPartWriter<IN, BucketID> openNew(
            final BucketID bucketId,
            final RecoverableFsDataOutputStream stream,
            final Path path,
            final long creationTime)
            throws IOException {

        Preconditions.checkNotNull(stream);
        Preconditions.checkNotNull(path);

        final BulkWriter<IN> writer = writerFactory.create(stream);
        return new DeltaBulkPartWriter<>(bucketId, stream, writer, creationTime);
    }

}
