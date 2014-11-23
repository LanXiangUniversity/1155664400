package lxu.lxmapreduce.task;

import java.io.Serializable;

/**
 * TaskStatus.java
 * Created by magl on 14/11/11.
 *
 * This is the status of a map or reduce task.
 */
public abstract class TaskStatus implements Serializable {
    public static final int PREP = 1;
    public static final int RUNNING = 2;
    public static final int SUCCEEDED = 3;
    public static final int FAILED = 4;

    private String taskTracker;
    private TaskAttemptID taskID;
    private int state;
    private int attemptFailedTime;

    public TaskStatus(TaskAttemptID taskID, String taskTracker, int state) {
        this.taskID = taskID;
        this.taskTracker = taskTracker;
        this.state = state;
    }

    public abstract boolean isMapTask();

    public TaskAttemptID getTaskID() {
        return taskID;
    }

    public void setTaskID(TaskAttemptID taskID) {
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
