package lxu.lxmapreduce.task;

import lxu.lxdfs.metadata.LocatedBlock;
import lxu.lxmapreduce.job.JobInProgress;
import lxu.lxmapreduce.job.JobTracker;
import lxu.lxmapreduce.task.map.MapTask;
import lxu.lxmapreduce.task.reduce.ReduceTask;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by magl on 14/11/10.
 */
public class TaskInProgress {
    private int numMaps;
    private TaskID taskID;
    private String jobID;
    private JobTracker jobTracker;
    private JobInProgress job;
    private TaskAttemptID successfulTaskID;
    private LocatedBlock locatedBlock;
    private int partition;
    // TaskAttemptID -> TaskStatus
    private Map<TaskAttemptID, TaskStatus> taskStatuses
            = new HashMap<TaskAttemptID, TaskStatus>();
    // TaskAttemptID -> TaskTrackerID
    private Map<TaskAttemptID, String> activeTasks
            = new HashMap<TaskAttemptID, String>();
    // All attempt ids of this task
    private Set<TaskAttemptID> allTaskAttempts = new HashSet<TaskAttemptID>();

    private int nextAttemptID = 0;

    /**
     * Constructor for MapTask
     */
    public TaskInProgress(String jobID, LocatedBlock locatedBlock,
                          JobTracker jobTracker, JobInProgress job, int partition) {
        this.jobID = jobID;
        this.locatedBlock = locatedBlock;
        this.jobTracker = jobTracker;
        this.job = job;
        this.partition = partition;
        this.taskID = new TaskID(jobID, true, partition);
    }

    /**
     * Constructor for ReduceTask
     */
    public TaskInProgress(String jobID, int numMaps, int partition,
                          JobTracker jobTracker, JobInProgress job) {
        this.jobID = jobID;
        this.numMaps = numMaps;
        this.partition = partition;
        this.jobTracker = jobTracker;
        this.job = job;
        this.taskID = new TaskID(jobID, false, partition);
    }

    public boolean updateStatus(TaskStatus status) {
        TaskAttemptID taskID = status.getTaskID();
        TaskStatus oldStatus = taskStatuses.get(taskID);
        if (oldStatus != null) {
            if (oldStatus.getState() == status.getState()) {
                return false;
            }
        }

        taskStatuses.put(taskID, status);

        return true;
    }

    public void setTaskCompleted(TaskAttemptID taskID) {
        taskStatuses.get(taskID).setState(TaskStatus.SUCCEEDED);
        successfulTaskID = taskID;
        activeTasks.remove(taskID);
    }

    public Task getTaskToRun(String taskTrackerName) {
        TaskAttemptID attemptID = new TaskAttemptID(taskID, nextAttemptID++);
        Task newTask = null;
        if (isMapTask()) {
            newTask = new MapTask(attemptID, partition, locatedBlock);
        } else {
            HashSet<String> locations = job.getAllMapTaskLocations();
            newTask = new ReduceTask(attemptID, partition, locatedBlock, locations);
        }

        activeTasks.put(attemptID, taskTrackerName);
        allTaskAttempts.add(attemptID);

        jobTracker.createTaskEntry(attemptID, taskTrackerName, this);

        return newTask;
    }

    public boolean isMapTask() {
        return locatedBlock != null;
    }

    public boolean isRunning() {
        return !activeTasks.isEmpty();
    }

    public boolean isRunnable() {
        return successfulTaskID == null;
    }

    public void clearActiveTasks() {
        this.activeTasks.clear();
    }

    public int getIdWithinJob() {
        return this.partition;
    }

    public LocatedBlock getLocatedBlock() {
        return locatedBlock;
    }

    public TaskID getTaskID() {
        return taskID;
    }

    public TaskAttemptID getSuccessfulTaskID() {
        return successfulTaskID;
    }

    public void setSuccessfulTaskID(TaskAttemptID successfulTaskID) {
        this.successfulTaskID = successfulTaskID;
    }
}
