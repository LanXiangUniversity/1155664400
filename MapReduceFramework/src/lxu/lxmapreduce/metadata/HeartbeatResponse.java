package lxu.lxmapreduce.metadata;

import lxu.lxmapreduce.task.TaskTrackerAction;

import java.io.Serializable;

/**
 * Created by magl on 14/11/10.
 */
public class HeartbeatResponse implements Serializable {
    private short responseID;
    private TaskTrackerAction[] actions;
}
