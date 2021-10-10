package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter;

public class DeltaInProgressPart<IN> {

    // TODO extend with bucket prefix
    private final String fileName;

    private final DeltaBulkPartWriter<IN, String> inProgressPart;

    public DeltaInProgressPart(String fileName,
                               DeltaBulkPartWriter<IN, String> inProgressPart) {
        this.fileName = fileName;
        this.inProgressPart = inProgressPart;
    }

    public String getFileName() {
        return fileName;
    }

    public DeltaBulkPartWriter<IN, String> getInProgressPart() {
        return inProgressPart;
    }


//
//    private final DeltaBulkPartWriter<IN, String> deltaBulkPartWriter;
//
//    public DeltaInProgressPart(String fileName,
//                               BulkPartWriter<IN, String> inProgressPart) {
//        this.fileName = fileName;
//        this.inProgressPart = inProgressPart;
//    }
//
//    public String getFileName() {
//        return fileName;
//    }
//
//    public OutputStreamBasedPartFileWriter<IN, String> getInProgressPart() {
//        return inProgressPart;
//    }


}
