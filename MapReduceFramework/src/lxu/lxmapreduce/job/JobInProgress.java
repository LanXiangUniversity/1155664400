package lxu.lxmapreduce.job;

import lxu.lxmapreduce.task.TaskInProgress;

import java.util.List;
import java.util.Map;

/**
 * Created by magl on 14/11/10.
 */
public class JobInProgress {
    private int jobID;
    private JobStatus jobStatus = null;
    private JobTracker jobTracker = null;
    // static job information:
    // map tasks, reduce tasks, etc
    private TaskInProgress[] maps = new TaskInProgress[0];
    private TaskInProgress[] reduces = new TaskInProgress[0];
    private int numMapTasks = 0;
    private int numReduceTasks = 0;

    // dynamic job information:
    private int runningMapTasks = 0;
    private int runningReduceTasks = 0;
    private int finishedMapTasks = 0;
    private int finishedReduceTasks = 0;
    private int failedMapTask = 0;
    private int failedReduceTask = 0;
    private Map<String, List<TaskInProgress>> nonRunningMapTasksMap;

    public JobInProgress(int jobID, JobTracker jobTracker) {
        this.jobID = jobID;
        this.jobTracker = jobTracker;
        this.jobStatus = new JobStatus(jobID, JobStatus.PREP);
        // TODO: Use configuration to initialize
        this.numMapTasks = 10;
        this.numReduceTasks = 10;
    }

    public void initTasks() {
        System.out.println("Initializing job: " + jobID);
        // TODO: Set numMapTasks according to splits
        // Init Map Tasks
        // TODO: getBlockLocations
        this.maps = new TaskInProgress[numMapTasks];
        for (int i = 0; i < numMapTasks; i++) {
            // TODO: Change to map task constructor
            this.maps[i] = new TaskInProgress();
        }
        nonRunningMapTasksMap = createCache();

        // Init Reduce Tasks
        this.reduces = new TaskInProgress[numReduceTasks];
        for (int i = 0; i < numReduceTasks; i++) {
            // TODO: Change to reduce task constructor
            this.reduces[i] = new TaskInProgress();
        }
    }

    /*
     * TODO: 1. getBlockLocations
     */
    public Map<String, List<TaskInProgress>> createCache() {
        return null;
    }

    public int getJobID() {
        return jobID;
    }

    public void setJobID(int jobID) {
        this.jobID = jobID;
    }

    public int getNumMapTasks() {
        return numMapTasks;
    }

    public void setNumMapTasks(int numMapTasks) {
        this.numMapTasks = numMapTasks;
    }

    public int getNumReduceTasks() {
        return numReduceTasks;
    }

    public void setNumReduceTasks(int numReduceTasks) {
        this.numReduceTasks = numReduceTasks;
    }

    public int getRunningMapTasks() {
        return runningMapTasks;
    }

    public void setRunningMapTasks(int runningMapTasks) {
        this.runningMapTasks = runningMapTasks;
    }

    public int getRunningReduceTasks() {
        return runningReduceTasks;
    }

    public void setRunningReduceTasks(int runningReduceTasks) {
        this.runningReduceTasks = runningReduceTasks;
    }

    public int getFinishedMapTasks() {
        return finishedMapTasks;
    }

    public void setFinishedMapTasks(int finishedMapTasks) {
        this.finishedMapTasks = finishedMapTasks;
    }

    public int getFinishedReduceTasks() {
        return finishedReduceTasks;
    }

    public void setFinishedReduceTasks(int finishedReduceTasks) {
        this.finishedReduceTasks = finishedReduceTasks;
    }

    public int getFailedMapTask() {
        return failedMapTask;
    }

    public void setFailedMapTask(int failedMapTask) {
        this.failedMapTask = failedMapTask;
    }

    public int getFailedReduceTask() {
        return failedReduceTask;
    }

    public void setFailedReduceTask(int failedReduceTask) {
        this.failedReduceTask = failedReduceTask;
    }

    public JobStatus getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }
}
