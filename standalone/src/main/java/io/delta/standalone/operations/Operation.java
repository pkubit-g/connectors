package io.delta.standalone.operations;

import java.util.Collections;
import java.util.Map;

public abstract class Operation {
    private final String name;
    private final Map<String, String> jsonEncodedValues;

    public Operation(String name, Map<String, String> jsonEncodedValues) {
        this.name = name;
        this.jsonEncodedValues = jsonEncodedValues;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getJsonEncodedValues() {
        return Collections.unmodifiableMap(jsonEncodedValues);
    }
}
