package lxu.lxmapreduce.tmp;

import java.text.NumberFormat;

/**
 * Created by Wei on 11/12/14.
 */
public class TaskID {
	private String taskID;
	private String jobID;
    private boolean isMapTask;

    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
    static {
        NUMBER_FORMAT.setMinimumIntegerDigits(5);
        NUMBER_FORMAT.setGroupingUsed(false);
    }

    public TaskID(String jobID, boolean isMapTask, int partition) {
        this.jobID = jobID;
        this.isMapTask = isMapTask;
        this.taskID = jobID + (isMapTask? "m-" : "r-") + NUMBER_FORMAT.format(partition);
    }

    public String getTaskID() {
        return taskID;
    }

    public void setTaskID(String taskID) {
        this.taskID = taskID;
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
}
