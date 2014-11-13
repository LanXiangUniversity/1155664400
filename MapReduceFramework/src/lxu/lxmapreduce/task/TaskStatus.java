package lxu.lxmapreduce.task;

/**
 * Created by magl on 14/11/11.
 */
public abstract class TaskStatus {
    public static final int RUNNING = 1;
    public static final int SUCCEEDED = 2;
    public static final int FAILED = 3;

    private String JobID;
    private String taskTracker;
    private String taskID;
    private int state;
    private int attemptFailedTime;

    public abstract boolean isMapTask();

    public String getJobID() {
        return JobID;
    }

    public void setJobID(String jobID) {
        JobID = jobID;
    }

    public String getTaskID() {
        return taskID;
    }

    public void setTaskID(String taskID) {
        this.taskID = taskID;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public String getTaskTracker() {
        return taskTracker;
    }

    public void setTaskTracker(String taskTracker) {
        this.taskTracker = taskTracker;
    }

    public void update(int state) {
        setState(state);
    }
}
