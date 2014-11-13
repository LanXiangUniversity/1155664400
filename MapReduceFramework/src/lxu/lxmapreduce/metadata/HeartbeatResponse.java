package lxu.lxmapreduce.metadata;

import lxu.lxmapreduce.task.Configuration;
import lxu.lxmapreduce.task.TaskTracker;
import lxu.lxmapreduce.task.TaskTrackerAction;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by magl on 14/11/10.
 */
public class HeartbeatResponse implements Serializable {
    private Configuration configuration;
    private short responseID;
    private String trackerName;
    private ArrayList<TaskTrackerAction> actions;

    public HeartbeatResponse(short responseID,
                             Configuration configuration,
                             ArrayList<TaskTrackerAction> actions) {
        this.responseID = responseID;
        this.configuration = configuration;
        this.actions = actions;
    }
}
