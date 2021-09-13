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
 *
 * Usage:
 * If possible, use one of the statically-defined operation names, e.g. new Operation(Operation.WRITE, ...)
 * If none of the names are applicable, then provide your own, e.g. new Operation("myEngineSpecificOperation", ...)
 */
public final class Operation {
    /** Recorded during batch inserts. */
    public static final String WRITE = "WRITE";

    /** Recorded during streaming inserts. */
    public static final String STREAMING_UPDATE = "STREAMING UPDATE";

    /** Recorded while deleting certain partitions. */
    public static final String DELETE = "DELETE";

    /** Recorded when truncating the table. */
    public static final String TRUNCATE = "TRUNCATE";

    /** Recorded when converting a table into a Delta table. */
    public static final String CONVERT = "CONVERT";

    public static final String MANUAL_UPDATE = "MANUAL_UPDATE";

    // TODO: the rest

    private final String name;
    private final Map<String, Object> parameters;
    private final Map<String, String> operationMetrics;
    private final Optional<String> userMetadata;

    public Operation(String name) {
        this(name, Collections.emptyMap(), Collections.emptyMap(), Optional.empty());
    }

    public Operation(String name, Map<String, Object> parameters) {
        this(name, parameters, Collections.emptyMap(), Optional.empty());
    }

    public Operation(String name, Map<String, Object> parameters, Map<String, String> operationMetrics) {
        this(name, parameters, operationMetrics, Optional.empty());
    }

    public Operation(String name, Map<String, Object> parameters, Map<String, String> operationMetrics,
                     Optional<String> userMetadata) {
        this.name = name;
        this.parameters = parameters;
        this.operationMetrics = operationMetrics;
        this.userMetadata = userMetadata;
    }

    public String getName() {
        return name;
    }

    public Map<String, Object> getParameters() {
        // TODO: be consistent with AddFile getter ternary
        return null == parameters ? null : Collections.unmodifiableMap(parameters);
    }

    public Map<String, String> getOperationMetrics() {
        return null == operationMetrics ? null : Collections.unmodifiableMap(operationMetrics);
    }

    public Optional<String> getUserMetadata() {
        return null == userMetadata ? Optional.empty() : userMetadata;
    }
}
