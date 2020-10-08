package io.delta.alpine;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public interface ReadOnlyLogStore {
    List<String> read(String path);
    List<String> read(Path path);
    Iterator<FileStatus> listFrom(String path);
    Iterator<FileStatus> listFrom(Path path);
}
