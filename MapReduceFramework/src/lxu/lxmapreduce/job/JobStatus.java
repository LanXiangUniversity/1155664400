package lxu.lxmapreduce.job;

import java.io.Serializable;

/**
 * JobStatus.java
 * Created by magl on 14/11/10.
 *
 * This is the wrapper of the status of a job.
 */
public class JobStatus implements Serializable {
    public static final int RUNNING = 1;
    public static final int SUCCEEDED = 2;
    public static final int FAILED = 3;
    public static final int KILLED = 4;
    public static final int PREP = 5;

    private String jobID;
    private int mapState;
    private int reduceState;
    private double mapProgress = 0.0;
    private double reduceProgress = 0.0;

    public JobStatus(String jobID, int mapState, int reduceState) {
        this.jobID = jobID;
        this.mapState = mapState;
        this.reduceState = reduceState;
    }

    public int getMapState() {
        return mapState;
    }

    public void setMapState(int mapState) {
        this.mapState = mapState;
    }

    public int getReduceState() {
        return reduceState;
    }

    public void setReduceState(int reduceState) {
        this.reduceState = reduceState;
    }

    public String getJobID() {
        return jobID;
    }

    public void setJobID(String jobID) {
        this.jobID = jobID;
    }

    public double getMapProgress() {
        return mapProgress;
    }

    public void setMapProgress(double mapProgress) {
        this.mapProgress = mapProgress;
    }

    public double getReduceProgress() {
        return reduceProgress;
    }

    public void setReduceProgress(double reduceProgress) {
        this.reduceProgress = reduceProgress;
    }

    public boolean isMapComplete() {
        return mapState == JobStatus.SUCCEEDED ||
                mapState == JobStatus.FAILED ||
                mapState == JobStatus.KILLED;
    }

    public boolean isReduceComplete() {
        return reduceState == JobStatus.SUCCEEDED ||
                reduceState == JobStatus.FAILED ||
                reduceState == JobStatus.KILLED;
    }

    public boolean isJobComplete() {
        return isMapComplete() && isReduceComplete();
    }

    public boolean isMapSuccessful() {
        return mapState == JobStatus.SUCCEEDED;
    }

    public boolean isReduceSuccessful() {
        return reduceState == JobStatus.SUCCEEDED;
    }

    public boolean isSuccessful() {
        return isMapSuccessful() && isReduceSuccessful();
    }
}
