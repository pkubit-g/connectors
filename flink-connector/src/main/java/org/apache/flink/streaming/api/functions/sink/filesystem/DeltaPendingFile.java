package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import javax.annotation.Nullable;


public class DeltaPendingFile {

    @Nullable
    private final String fileName;

    @Nullable
    private final InProgressFileWriter.PendingFileRecoverable pendingFile;

    @Nullable
    private final long recordCount;

    @Nullable
    private final long fileSize;


    public DeltaPendingFile(@Nullable String fileName,
                            @Nullable InProgressFileWriter.PendingFileRecoverable pendingFile,
                            long recordCount,
                            long fileSize) {
        this.fileName = fileName;
        this.pendingFile = pendingFile;
        this.fileSize = fileSize;
        this.recordCount = recordCount;
    }

    @Nullable
    public String getFileName() {
        return fileName;
    }

    @Nullable
    public InProgressFileWriter.PendingFileRecoverable getPendingFile() {
        return pendingFile;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getRecordCount() {
        return recordCount;
    }
}
