package io.delta.standalone.operations;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public abstract class Operation {
    private final String name;
    private final Map<String, String> jsonEncodedValues;
    private final Optional<String> userMetadata;

    // TODO: any way to use java-equivalent of val parameters: Map[String, Any] ?

    public Operation(String name, Map<String, String> jsonEncodedValues) {
        this.name = name;
        this.jsonEncodedValues = jsonEncodedValues;
        this.userMetadata = Optional.empty();
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
