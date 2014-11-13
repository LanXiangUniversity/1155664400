package lxu.lxmapreduce.metadata;

import lxu.lxmapreduce.task.TaskStatus;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by magl on 14/11/10.
 */
public class TaskTrackerStatus implements Serializable {
    private String trackerName;
    private String hostIP;
    private long lastSeen;
    // Task Status List
    private LinkedList<TaskStatus> taskStatuses;

    public String getTrackerName() {
        return this.trackerName;
    }

    public long getLastSeen() {
        return this.lastSeen;
    }

    public void setLastSeen(long lastSeen) {
        this.lastSeen = lastSeen;
    }

    public List<TaskStatus> getTaskReport() {
        return this.taskStatuses;
    }
}
