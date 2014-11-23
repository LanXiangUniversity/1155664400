package lxu.lxmapreduce.metadata;

import lxu.lxmapreduce.task.Task;

import java.io.Serializable;

/**
 * LaunchTaskAction.java
 * Created by magl on 14/11/13.
 *
 * Ask TaskTracker to launch a new task.
 */
public class LaunchTaskAction extends TaskTrackerAction implements Serializable {
    private Task task;

    public LaunchTaskAction(ActionType actionType) {
        super(ActionType.LAUNCH_TASK);
    }

    public LaunchTaskAction(Task task) {
        super(ActionType.LAUNCH_TASK);
        this.task = task;
    }

    public Task getTask() {
        return this.task;
    }
}