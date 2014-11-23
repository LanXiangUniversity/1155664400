package lxu.lxmapreduce.task.map;

import lxu.lxmapreduce.task.TaskAttemptID;
import lxu.lxmapreduce.task.TaskStatus;

import java.io.Serializable;

/**
 * MapTaskStatus.java
 * Created by magl on 14/11/13.
 *
 * The status of a map task.
 */
public class MapTaskStatus extends TaskStatus implements Serializable {

    public MapTaskStatus(TaskAttemptID taskID, String taskTracker, int state) {
        super(taskID, taskTracker, state);
    }

    @Override
    public boolean isMapTask() {
        return true;
    }
}
