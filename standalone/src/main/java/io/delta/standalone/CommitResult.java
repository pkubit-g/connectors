package io.delta.standalone;

public class CommitResult {
    public final long version;

    public CommitResult(long version) {
        this.version = version;
    }

    public long getVersion() {
        return version;
    }
}
