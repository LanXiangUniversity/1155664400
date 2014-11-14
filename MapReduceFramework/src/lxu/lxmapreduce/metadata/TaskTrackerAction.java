package lxu.lxmapreduce.metadata;

import java.io.Serializable;

/**
 * Created by magl on 14/11/10.
 */
public abstract class TaskTrackerAction implements Serializable {
    // Reinit Tracker
    // Launch Task
    // Kill Task
    // Kill Job
    // Commit Task
    public static enum ActionType {
        LAUNCH_TASK, COMMIT_TASK
    }

    private ActionType actionType;

    protected TaskTrackerAction(ActionType actionType) {
        this.actionType = actionType;
    }

    public ActionType getActionType() {
        return actionType;
    }
}
