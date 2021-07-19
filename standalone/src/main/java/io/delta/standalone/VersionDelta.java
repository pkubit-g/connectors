package io.delta.standalone;

import io.delta.standalone.actions.Action;

import java.util.List;

public class VersionDelta {
    private final long version;
    private final List<Action> actions;

    public VersionDelta(long version, List<Action> actions) {
        this.version = version;
        this.actions = actions;
    }

    public long getVersion() {
        return version;
    }

    public List<Action> getActions() {
        return actions;
    }
}
