package io.delta.alpine.actions;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;

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

    public String getPath() {
        return path;
    }

    public URI getPathAsUri() throws URISyntaxException {
        return new URI(path);
    }

    public Map<String, String> getPartitionValues() {
        return Collections.unmodifiableMap(partitionValues);
    }

    public long getSize() {
        return size;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public boolean isDataChange() {
        return dataChange;
    }

    public String getStats() {
        return stats;
    }

    public Map<String, String> getTags() {
        return Collections.unmodifiableMap(tags);
    }
}
