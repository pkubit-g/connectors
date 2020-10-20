package io.delta.alpine.actions;

import java.util.Collections;
import java.util.Map;

public final class Format {
    private final String provider;
    private final Map<String, String> options;

    public Format(String provider, Map<String, String> options) {
        this.provider = provider;
        this.options = options;
    }

    public String getProvider() {
        return provider;
    }

    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }
}
