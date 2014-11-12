package lxu.lxmapreduce.metadata;

import lxu.lxmapreduce.task.TaskStatus;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * Created by magl on 14/11/10.
 */
public class TaskTrackerStatus implements Serializable {
    private String trackerName;
    private String hostIP;
    // Task Status List
    private LinkedList<TaskStatus> taskStatuses;
}
