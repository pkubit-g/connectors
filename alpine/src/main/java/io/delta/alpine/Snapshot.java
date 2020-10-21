package io.delta.alpine;

import java.util.List;

import io.delta.alpine.actions.AddFile;
import io.delta.alpine.actions.Metadata;
import io.delta.alpine.data.CloseableIterator;
import io.delta.alpine.data.RowRecord;
import org.apache.hadoop.fs.Path;

public interface Snapshot {
    List<AddFile> getAllFiles();
    Metadata getMetadata();

    Path getPath();
    long getVersion();
    DeltaLog getDeltaLog();
    long getTimestamp();
    int getNumOfFiles();
    CloseableIterator<RowRecord> open();
}
