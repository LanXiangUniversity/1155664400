package lxu.lxmapreduce.job;

import lxu.lxmapreduce.metadata.TaskTrackerStatus;
import lxu.lxmapreduce.task.Task;
import lxu.lxmapreduce.task.TaskTracker;

import java.util.List;

/**
 * Created by magl on 14/11/10.
 */
public class TaskScheduler {
    // use job tracker to get job information
    private JobTracker jobTracker = null;

    public void setJobTracker(JobTracker jobTracker) {
        this.jobTracker = jobTracker;
    }

    public List<Task> assignTasks(TaskTrackerStatus taskTrackerStatus) {
        return null;
    }
}
