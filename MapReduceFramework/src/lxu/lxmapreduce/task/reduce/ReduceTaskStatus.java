package lxu.lxmapreduce.task.reduce;

import lxu.lxmapreduce.task.TaskAttemptID;
import lxu.lxmapreduce.task.TaskStatus;

import java.io.Serializable;

/**
 * Created by magl on 14/11/13.
 */
public class ReduceTaskStatus extends TaskStatus implements Serializable {

    public ReduceTaskStatus(TaskAttemptID taskID, String taskTracker, int state) {
        super(taskID, taskTracker, state);
    }

    @Override
    public boolean isMapTask() {
        return false;
    }
}
