package lxu.lxmapreduce.task.map;

import lxu.lxmapreduce.task.TaskAttemptID;
import lxu.lxmapreduce.task.TaskStatus;

import java.io.Serializable;

/**
 * Created by magl on 14/11/13.
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
