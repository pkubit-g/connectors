package io.delta.standalone.operations;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public abstract class Operation {
    private final String name;
    private final Map<String, String> jsonEncodedValues;
    private final Optional<String> userMetadata;
    // TODO: operationMetrics?

    public Operation(String name) {
        this(name, Collections.emptyMap());
    }

    public Operation(String name, Map<String, String> jsonEncodedValues) {
        this(name, jsonEncodedValues, Optional.empty());
    }

    public Operation(String name, Map<String, String> jsonEncodedValues, Optional<String> userMetadata) {
        this.name = name;
        this.jsonEncodedValues = jsonEncodedValues;
        this.userMetadata = userMetadata;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getJsonEncodedValues() {
        return null == jsonEncodedValues ? null : Collections.unmodifiableMap(jsonEncodedValues);
    }

    public Optional<String> getUserMetadata() {
        return userMetadata;
    }
}
