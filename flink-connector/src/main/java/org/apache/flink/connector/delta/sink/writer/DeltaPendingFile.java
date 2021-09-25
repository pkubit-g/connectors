package org.apache.flink.connector.delta.sink.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import javax.annotation.Nullable;


public class DeltaPendingFile {

    @Nullable
    private final Path path;

    @Nullable
    private final InProgressFileWriter.PendingFileRecoverable pendingFile;

    public DeltaPendingFile(@Nullable Path path,
                            @Nullable InProgressFileWriter.PendingFileRecoverable pendingFile) {
        this.path = path;
        this.pendingFile = pendingFile;
    }

    @Nullable
    public Path getPath() {
        return path;
    }

    @Nullable
    public InProgressFileWriter.PendingFileRecoverable getPendingFile() {
        return pendingFile;
    }

}
