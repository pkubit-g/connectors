package io.delta.standalone.actions;

import java.util.Optional;

public class RemoveFile implements FileAction {
    private final String path;
    private final Optional<Long> deletionTimestamp;
    private final boolean dataChange;

    public RemoveFile(String path, Optional<Long> deletionTimestamp, boolean dataChange) {
        this.path = path;
        this.deletionTimestamp = deletionTimestamp;
        this.dataChange = dataChange;
    }

    @Override
    public String getPath() {
        return path;
    }

    public Optional<Long> getDeletionTimestamp() {
        return deletionTimestamp;
    }

    @Override
    public boolean isDataChange() {
        return dataChange;
    }
}
