package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

public class DeltaBulkPartWriter<IN, BucketID> extends OutputStreamBasedPartFileWriter<IN, BucketID> {

    private final BulkWriter<IN> writer;

    private boolean closed = false;

    public DeltaBulkPartWriter(
            final BucketID bucketId,
            final RecoverableFsDataOutputStream currentPartStream,
            final BulkWriter<IN> writer,
            final long creationTime) {
        super(bucketId, currentPartStream, creationTime);
        this.writer = Preconditions.checkNotNull(writer);
    }

    @Override
    public void write(IN element, long currentTime) throws IOException {
        writer.addElement(element);
        markWrite(currentTime);
    }

    @Override
    public InProgressFileRecoverable persist() {
        throw new UnsupportedOperationException(
                "Bulk Part Writers do not support \"pause and resume\" operations.");
    }

    @Override
    public PendingFileRecoverable closeForCommit() throws IOException {
        if (!closed) {
            closeWriter();
        }

        return super.closeForCommit();
    }

    public void closeWriter() throws IOException {
        writer.flush();
        writer.finish();
        closed = true;
    }

}
