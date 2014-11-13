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
    public static final int PREP = 5;

    private String jobID;
    private int mapState;
    private int reduceState;

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

    public boolean isMapComplete() {
        return mapState == JobStatus.SUCCEEDED;
    }

    public boolean isReduceComplete() {
        return reduceState == JobStatus.SUCCEEDED;
    }

    public boolean isJobComplete() {
        return isMapComplete() && isReduceComplete();
    }
}
