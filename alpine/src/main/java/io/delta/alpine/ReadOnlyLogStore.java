package io.delta.alpine;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public abstract class ReadOnlyLogStore {

    /** Read the given `path` */
    public final List<String> read(String path) {
        return read(new Path(path));
    }

    /** Read the given `path` */
    public abstract List<String> read(Path path);

    /**
     * List the paths in the same directory that are lexicographically greater or equal to
     * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
     */
    public final Iterator<FileStatus> listFrom(String path) {
        return listFrom(new Path(path));
    }

    /**
     * List the paths in the same directory that are lexicographically greater or equal to
     * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
     */
    public abstract Iterator<FileStatus> listFrom(Path path);
}
