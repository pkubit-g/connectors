package io.delta.alpine.actions;

import java.util.Map;

public class Format {
    private String provider;
    private Map<String, String> options;

    public Format(String provider, Map<String, String> options) {
        this.provider = provider;
        this.options = options;
    }

    public String getProvider() {
        return provider;
    }

    public Map<String, String> getOptions() {
        return options;
    }
}
