package lxu.lxmapreduce.task;

import lxu.lxmapreduce.metadata.TaskTrackerStatus;

/**
 * Created by Wei on 11/12/14.
 */
public class TaskTracker {
    private String taskTrackerName;
    private TaskTrackerStatus status;

    public TaskTrackerStatus getStatus() {
        return status;
    }

    public void setStatus(TaskTrackerStatus status) {
        this.status = status;
    }
}
