package lxu.lxmapreduce.metadata;

import java.io.Serializable;

/**
 * Created by magl on 14/11/10.
 */
public abstract class TaskTrackerAction implements Serializable {
    // Reinit Tracker
    // Launch Task
    // Kill Task
    // Commit Task

    private ActionType actionType;

    protected TaskTrackerAction(ActionType actionType) {
        this.actionType = actionType;
    }

    public ActionType getActionType() {
        return actionType;
    }

    public static enum ActionType {
        LAUNCH_TASK, COMMIT_TASK
    }
}
