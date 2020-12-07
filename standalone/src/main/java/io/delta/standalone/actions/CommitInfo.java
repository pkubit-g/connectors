package io.delta.standalone.actions;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class CommitInfo {
    private final Optional<Long> version;
    private final Timestamp timestamp;
    private final Optional<String> userId;
    private final Optional<String> userName;
    private final String operation;
    private final Map<String, String> operationParameters;
    private final Optional<JobInfo> jobInfo;
    private final Optional<String> notebookId;
    private final Optional<String> clusterId;
    private final Optional<Long> readVersion;
    private final Optional<String> isolationLevel;
    private final Optional<Boolean> isBlindAppend;
    private final Optional<Map<String, String>> operationMetrics;
    private final Optional<String> userMetadata;

    public CommitInfo(Optional<Long> version, Timestamp timestamp, Optional<String> userId,
                      Optional<String> userName, String operation,
                      Map<String, String> operationParameters, Optional<JobInfo> jobInfo,
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
        this.jobInfo = jobInfo;
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

    public Optional<JobInfo> getJobInfo() {
        return jobInfo;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommitInfo that = (CommitInfo) o;
        return Objects.equals(version, that.version) &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(userId, that.userId) &&
                Objects.equals(userName, that.userName) &&
                Objects.equals(operation, that.operation) &&
                Objects.equals(operationParameters, that.operationParameters) &&
                Objects.equals(jobInfo, that.jobInfo) &&
                Objects.equals(notebookId, that.notebookId) &&
                Objects.equals(clusterId, that.clusterId) &&
                Objects.equals(readVersion, that.readVersion) &&
                Objects.equals(isolationLevel, that.isolationLevel) &&
                Objects.equals(isBlindAppend, that.isBlindAppend) &&
                Objects.equals(operationMetrics, that.operationMetrics) &&
                Objects.equals(userMetadata, that.userMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, timestamp, userId, userName, operation, operationParameters,
                jobInfo, notebookId, clusterId, readVersion, isolationLevel, isBlindAppend,
                operationMetrics, userMetadata);
    }
}
