package lxu.lxmapreduce.task;

import lxu.lxmapreduce.tmp.TaskID;

/**
 * Created by magl on 14/11/13.
 */
public class TaskAttemptID {
    private TaskID taskID;
    private int attemptID;

    public TaskAttemptID(TaskID taskID, int attemptID) {
        this.taskID = taskID;
        this.attemptID = attemptID;
    }

    /** Returns the jobID that this task attempt belongs to */
    public String getJobID() {
        return taskID.getJobID();
    }

    /** Returns the TaskID object that this task attempt belongs to */
    public TaskID getTaskID() {
        return this.taskID;
    }

    /**Returns whether this TaskAttemptID is a map ID */
    public boolean isMap() {
        return taskID.isMapTask();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskAttemptID that = (TaskAttemptID) o;

        if (attemptID != that.attemptID) return false;
        if (!taskID.equals(that.taskID)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = taskID != null ? taskID.hashCode() : 0;
        result = 31 * result + attemptID;
        return result;
    }

    @Override
    public String toString() {
        return taskID.toString() + "_" + attemptID;
    }
}
