package io.delta.alpine;

import java.util.List;

import io.delta.alpine.actions.AddFile;
import io.delta.alpine.actions.Metadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public interface Snapshot {
    List<AddFile> getAllFiles();
    int getMinReaderVersion();
    Metadata getMetadata();

    Configuration getHadoopConf();
    Path getPath();
    long getVersion();
    DeltaLog getDeltaLog();
    long getTimestamp();
    int getNumOfFiles();
//    CloseableIterator<RowParquetRecord> open();
}
