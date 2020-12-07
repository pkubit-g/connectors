package io.delta.standalone.actions;

public class JobInfo {
    private final String jobId;
    private final String jobName;
    private final String runId;
    private final String jobOwnerId;
    private final String triggerType;

    public JobInfo(String jobId, String jobName, String runId, String jobOwnerId, String triggerType) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.runId = runId;
        this.jobOwnerId = jobOwnerId;
        this.triggerType = triggerType;
    }

    public String getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public String getRunId() {
        return runId;
    }

    public String getJobOwnerId() {
        return jobOwnerId;
    }

    public String getTriggerType() {
        return triggerType;
    }
}
