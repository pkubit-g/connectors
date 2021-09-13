package io.delta.standalone;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * An operation that can be performed on a Delta table.
 *
 * An operation is tracked as the first line in delta logs, and powers `DESCRIBE HISTORY` for Delta
 * tables.
 */
public final class Operation {

    /**
     * Supported operation types.
     */
    public enum Name {
        /** Recorded during batch inserts. */
        WRITE,

        /** Recorded during streaming inserts. */
        STREAMING_UPDATE,

        /** Recorded while deleting certain partitions. */
        DELETE,

        /** Recorded when truncating the table. */
        TRUNCATE,

        /** Recorded when converting a table into a Delta table. */
        CONVERT,

        // TODO: the rest

        MANUAL_UPDATE
    }

    private final Name name;
    private final Map<String, Object> parameters;
    private final Map<String, String> operationMetrics;
    private final Optional<String> userMetadata;

    public Operation(Name name) {
        this(name, Collections.emptyMap(), Collections.emptyMap(), Optional.empty());
    }

    public Operation(Name name, Map<String, Object> parameters) {
        this(name, parameters, Collections.emptyMap(), Optional.empty());
    }

    public Operation(Name name, Map<String, Object> parameters, Map<String, String> operationMetrics) {
        this(name, parameters, operationMetrics, Optional.empty());
    }

    public Operation(Name name, Map<String, Object> parameters, Map<String, String> operationMetrics,
                     Optional<String> userMetadata) {
        this.name = name;
        this.parameters = parameters;
        this.operationMetrics = operationMetrics;
        this.userMetadata = userMetadata;
    }

    /**
     * TODO
     * @return
     */
    public Name getName() {
        return name;
    }

    /**
     * TODO
     * @return
     */
    public Map<String, Object> getParameters() {
        return null == parameters ? null : Collections.unmodifiableMap(parameters);
    }

    /**
     * TODO
     * @return
     */
    public Map<String, String> getOperationMetrics() {
        return null == operationMetrics ? null : Collections.unmodifiableMap(operationMetrics);
    }

    /**
     * TODO
     * @return
     */
    public Optional<String> getUserMetadata() {
        return null == userMetadata ? Optional.empty() : userMetadata;
    }
}
