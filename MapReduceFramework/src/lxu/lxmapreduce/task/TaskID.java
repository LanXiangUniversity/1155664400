package lxu.lxmapreduce.task;

import java.io.Serializable;

/**
 * Created by Wei on 11/12/14.
 */
public class TaskID implements Serializable {
    private int partition;
    private String jobID;
    private boolean isMapTask;

    public TaskID(String jobID, boolean isMapTask, int partition) {
        this.jobID = jobID;
        this.isMapTask = isMapTask;
        this.partition = partition;
    }

    public String getJobID() {
        return jobID;
    }

    public void setJobID(String jobID) {
        this.jobID = jobID;
    }

    public boolean isMapTask() {
        return isMapTask;
    }

    public void setMapTask(boolean isMapTask) {
        this.isMapTask = isMapTask;
    }

    @Override
    public String toString() {
        return jobID + (isMapTask ? "_m-" : "_r-") + partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskID taskID = (TaskID) o;

        if (isMapTask != taskID.isMapTask) return false;
        if (partition != taskID.partition) return false;
        if (!jobID.equals(taskID.jobID)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = partition;
        result = 31 * result + jobID.hashCode();
        result = 31 * result + (isMapTask ? 1 : 0);
        return result;
    }
}
