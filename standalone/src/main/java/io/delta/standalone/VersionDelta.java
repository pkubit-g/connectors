package io.delta.standalone;

import io.delta.standalone.actions.Action;

import java.util.Collections;
import java.util.List;

/**
 * {@link VersionDelta} is the representation of all actions (changes) to the Delta Table
 * at a specific table version.
 */
public class VersionDelta {
    private final long version;
    private final List<Action> actions;

    public VersionDelta(long version, List<Action> actions) {
        this.version = version;
        this.actions = actions;
    }

    /**
     * @return the table version at which these actions occured
     */
    public long getVersion() {
        return version;
    }

    /**
     * @return an unmodifiable {@code List} of the actions for this table version
     */
    public List<Action> getActions() {
        return Collections.unmodifiableList(actions);
    }
}
