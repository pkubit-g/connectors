package io.delta.alpine.actions;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;

/**
 * Adds a new file to the table. When multiple {@code AddFile} file actions
 * are seen with the same {@code path} only the metadata from the last one is
 * kept.
 */
public final class AddFile {
    private final String path;
    private final Map<String, String> partitionValues;
    private final long size;
    private final long modificationTime;
    private final boolean dataChange;
    private final String stats;
    private final Map<String, String> tags;

    public AddFile(String path, Map<String, String> partitionValues, long size,
                   long modificationTime, boolean dataChange, String stats,
                   Map<String, String> tags) {
        this.path = path;
        this.partitionValues = partitionValues;
        this.size = size;
        this.modificationTime = modificationTime;
        this.dataChange = dataChange;
        this.stats = stats;
        this.tags = tags;
    }

    /**
     * @return the path for data file that this {@code AddFile} instance represents
     */
    public String getPath() {
        return path;
    }

    /**
     * @return the URI for data file that this {@code AddFile} instance represents
     */
    public URI getPathAsUri() throws URISyntaxException {
        return new URI(path);
    }

    /**
     * @return a {@code Map} of {@code partitionKey} to {@code partitionValue}
     */
    public Map<String, String> getPartitionValues() {
        return Collections.unmodifiableMap(partitionValues);
    }

    /**
     * @return the size in bytes of the data file
     */
    public long getSize() {
        return size;
    }

    /**
     * @return the time in milliseconds that the data file was last modified or created
     */
    public long getModificationTime() {
        return modificationTime;
    }

    /**
     * @return whether any data was changed as a result of this file being created
     */
    public boolean isDataChange() {
        return dataChange;
    }

    /**
     * @return statistics about the data file, in a {@code String} representation of JSON
     */
    public String getStats() {
        return stats;
    }

    /**
     * @return an unmodifiable {@code Map} of user-defined metadata for this file
     */
    public Map<String, String> getTags() {
        return Collections.unmodifiableMap(tags);
    }
}
