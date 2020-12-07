package io.delta.standalone.actions;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class CommitInfo {
    private final Optional<Long> version;
    private final Timestamp timestamp;
    private final Optional<String> userId;
    private final Optional<String> userName;
    private final String operation;
    private final Map<String, String> operationParameters;
    private final Optional<JobInfo> job;
    private final Optional<String> notebookId;
    private final Optional<String> clusterId;
    private final Optional<Long> readVersion;
    private final Optional<String> isolationLevel;
    private final Optional<Boolean> isBlindAppend;
    private final Optional<Map<String, String>> operationMetrics;
    private final Optional<String> userMetadata;

    public CommitInfo(Optional<Long> version, Timestamp timestamp, Optional<String> userId,
                      Optional<String> userName, String operation,
                      Map<String, String> operationParameters, Optional<JobInfo> job,
                      Optional<String> notebookId, Optional<String> clusterId,
                      Optional<Long> readVersion, Optional<String> isolationLevel,
                      Optional<Boolean> isBlindAppend,
                      Optional<Map<String, String>> operationMetrics,
                      Optional<String> userMetadata) {
        this.version = version;
        this.timestamp = timestamp;
        this.userId = userId;
        this.userName = userName;
        this.operation = operation;
        this.operationParameters = operationParameters;
        this.job = job;
        this.notebookId = notebookId;
        this.clusterId = clusterId;
        this.readVersion = readVersion;
        this.isolationLevel = isolationLevel;
        this.isBlindAppend = isBlindAppend;
        this.operationMetrics = operationMetrics;
        this.userMetadata = userMetadata;
    }

    public Optional<Long> getVersion() {
        return version;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public Optional<String> getUserId() {
        return userId;
    }

    public Optional<String> getUserName() {
        return userName;
    }

    public String getOperation() {
        return operation;
    }

    public Map<String, String> getOperationParameters() {
        return Collections.unmodifiableMap(operationParameters);
    }

    public Optional<JobInfo> getJob() {
        return job;
    }

    public Optional<String> getNotebookId() {
        return notebookId;
    }

    public Optional<String> getClusterId() {
        return clusterId;
    }

    public Optional<Long> getReadVersion() {
        return readVersion;
    }

    public Optional<String> getIsolationLevel() {
        return isolationLevel;
    }

    public Optional<Boolean> getIsBlindAppend() {
        return isBlindAppend;
    }

    public Optional<Map<String, String>> getOperationMetrics() {
        if (operationMetrics.isPresent()) {
            return Optional.of(Collections.unmodifiableMap(operationMetrics.get()));
        }
        return operationMetrics;
    }

    public Optional<String> getUserMetadata() {
        return userMetadata;
    }
}
