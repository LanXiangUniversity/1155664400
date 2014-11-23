package lxu.lxmapreduce.metadata;

import java.io.Serializable;

/**
 * TaskTrackerAction.java
 * Created by magl on 14/11/10.
 *
 * The base class for all Action.
 */
public abstract class TaskTrackerAction implements Serializable {
    private ActionType actionType;

    protected TaskTrackerAction(ActionType actionType) {
        this.actionType = actionType;
    }

    public ActionType getActionType() {
        return actionType;
    }

    public static enum ActionType {
        LAUNCH_TASK
    }
}
