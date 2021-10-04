package org.apache.flink.connector.delta.sink.writer;

import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter;

public class DeltaInProgressPart<IN> {

    // TODO extend with bucket prefix
    private final String fileName;

    private final OutputStreamBasedPartFileWriter<IN, String> inProgressPart;

    public DeltaInProgressPart(String fileName,
                               OutputStreamBasedPartFileWriter<IN, String> inProgressPart) {
        this.fileName = fileName;
        this.inProgressPart = inProgressPart;
    }

    public String getFileName() {
        return fileName;
    }

    public OutputStreamBasedPartFileWriter<IN, String> getInProgressPart() {
        return inProgressPart;
    }


}
