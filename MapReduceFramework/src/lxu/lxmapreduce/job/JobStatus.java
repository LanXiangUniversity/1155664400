package lxu.lxmapreduce.job;

import java.io.Serializable;

/**
 * Created by magl on 14/11/10.
 */
public class JobStatus implements Serializable {
    public static final int RUNNING = 1;
    public static final int SUCCEEDED = 2;
    public static final int FAILED = 3;
    public static final int KILLED = 4;

    private int jobID;
    private int runState;

    public JobStatus(int jobID, int runState) {
        this.jobID = jobID;
        this.runState = runState;
    }

    public int getRunState() {
        return runState;
    }

    public void setRunState(int runState) {
        this.runState = runState;
    }

    public int getJobID() {
        return jobID;
    }

    public void setJobID(int jobID) {
        this.jobID = jobID;
    }

    public boolean isJobComplete() {
        return (runState == JobStatus.SUCCEEDED ||
                runState == JobStatus.FAILED ||
                runState == JobStatus.KILLED);
    }
}
