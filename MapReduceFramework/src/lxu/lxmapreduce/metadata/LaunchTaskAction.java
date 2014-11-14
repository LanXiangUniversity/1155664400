package lxu.lxmapreduce.metadata;

import lxu.lxmapreduce.task.Task;

/**
 * Created by magl on 14/11/13.
 */
public class LaunchTaskAction extends TaskTrackerAction {
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
