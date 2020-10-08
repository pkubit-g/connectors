package io.delta.alpine.actions;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class AddFile {
    private String path;
    private Map<String, String> partitionValues;
    private long size;
    private long modificationTime;
    private boolean dataChange;
    private String stats;
    private Map<String, String> tags;

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
        return partitionValues;
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
        return tags;
    }

    public void setStatsNull() {
        this.stats = null;
    }

    public void setTagsNull() {
        this.tags = null;
    }
}
