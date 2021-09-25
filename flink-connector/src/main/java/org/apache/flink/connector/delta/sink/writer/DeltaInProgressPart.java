package org.apache.flink.connector.delta.sink.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;


public class DeltaInProgressPart<IN> {

    private final Path path;

    private final InProgressFileWriter<IN, String> inProgressPart;

    public DeltaInProgressPart(Path path,
                               InProgressFileWriter<IN, String> inProgressPart) {
        this.path = path;
        this.inProgressPart = inProgressPart;
    }

    public Path getPath() {
        return path;
    }

    public InProgressFileWriter<IN, String> getInProgressPart() {
        return inProgressPart;
    }

}
