package lxu.lxmapreduce.task;

import lxu.lxmapreduce.job.JobInProgress;
import lxu.lxmapreduce.job.JobTracker;

import java.util.Map;
import java.util.Set;

/**
 * Created by magl on 14/11/10.
 */
public class TaskInProgress {
    // int numMaps
    private JobTracker jobTracker;
    private JobInProgress job;
    // TaskID -> TaskStatus
    private Map<String, TaskStatus> taskStatuses;
    private String successfulTaskID;

    public TaskInProgress() {
    }

    public boolean updateStatus(TaskStatus status) {
        String taskID = status.getTaskID();
        TaskStatus oldStatus = taskStatuses.get(taskID);
        if (oldStatus.getState() == status.getState()) {
            return false;
        }

        taskStatuses.get(taskID).update(status.getState());

        return true;
    }

    public void setTaskCompleted(String taskID) {
        taskStatuses.get(taskID).setState(TaskStatus.SUCCEEDED);
        successfulTaskID = taskID;
    }

    // TODO:
    public boolean isMapTask() {
        return false;
    }
}
