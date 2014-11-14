package lxu.lxmapreduce.job;

import lxu.lxdfs.metadata.DataNodeDescriptor;
import lxu.lxdfs.metadata.LocatedBlock;
import lxu.lxdfs.metadata.LocatedBlocks;
import lxu.lxmapreduce.metadata.TaskTrackerStatus;
import lxu.lxmapreduce.task.*;

import java.util.*;

/**
 * Created by magl on 14/11/10.
 */
public class JobInProgress {
    private String jobID;
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
    // HostIP -> TaskInProgress
    private Map<String, List<TaskInProgress>> nonRunningMapTasksMap;
    private Map<String, Set<TaskInProgress>> runningMapTasksMap;

    private boolean tasksInited = false;

    public JobInProgress(String jobID, JobTracker jobTracker) {
        this.jobID = jobID;
        this.jobTracker = jobTracker;
        this.jobStatus = new JobStatus(jobID, JobStatus.PREP, JobStatus.PREP);
        // TODO: Use configuration to initialize
        this.numMapTasks = 10;
        this.numReduceTasks = 10;
    }

    public void initTasks() {
        if (tasksInited || isComplete()) {
            return;
        }

        System.out.println("Initializing job: " + jobID);
        LocatedBlock[] allBlocks = getFileBlocks("fileName");
        numMapTasks = allBlocks.length;
        // Init Map Tasks
        this.maps = new TaskInProgress[numMapTasks];
        for (int i = 0; i < numMapTasks; i++) {
            this.maps[i] = new TaskInProgress(jobID, allBlocks[i], jobTracker, this, i);
        }
        nonRunningMapTasksMap = createCache(allBlocks);

        // Init Reduce Tasks
        this.reduces = new TaskInProgress[numReduceTasks];
        for (int i = 0; i < numReduceTasks; i++) {
            this.reduces[i] = new TaskInProgress(jobID, numMapTasks, i, jobTracker, this);
        }

        tasksInited = true;
    }

    // TODO: connect to namenode to get all blocks of given file
    public LocatedBlock[] getFileBlocks(String fileName) {
        return null;
    }

    public void updateTaskStatus(TaskInProgress tip, TaskStatus status) {
        // if task has complete and status is succeed, ignore status
        /*
        if (tip.isComplete() && status.getState() == SUCCEED) {
            return;
        }
         */
        boolean changed = tip.updateStatus(status);
        if (changed) {
            int state = status.getState();

            if (state == TaskStatus.SUCCEEDED) {
                handleSucceedTask(tip, status);
            } else if (state == TaskStatus.FAILED) {
                // TODO: handle failed task
            }
        }
    }

    public void handleSucceedTask(TaskInProgress tip, TaskStatus status) {
        String taskID = status.getTaskID();
        System.out.println("Task '" + taskID + "' has completed!");
        tip.setTaskCompleted(taskID);
        if (tip.isMapTask()) {
            runningMapTasks--;
            finishedMapTasks++;
            if (finishedMapTasks == numMapTasks) {
                this.jobStatus.setMapState(JobStatus.SUCCEEDED);
            } else {
                this.jobStatus.setMapState(JobStatus.RUNNING);
            }
        } else {
            runningReduceTasks--;
            finishedReduceTasks++;
            if (finishedReduceTasks == numReduceTasks) {
                this.jobStatus.setReduceState(JobStatus.SUCCEEDED);
            } else {
                this.jobStatus.setReduceState(JobStatus.RUNNING);
            }
        }
    }

    public Map<String, List<TaskInProgress>> createCache(LocatedBlock[] blocks) {
        Map<String, List<TaskInProgress>> cache = new HashMap<String, List<TaskInProgress>>();

        // for each block
        for (int i = 0; i < blocks.length; i++) {
            HashSet<DataNodeDescriptor> locations = blocks[i].getLocations();
            // for each location
            for (DataNodeDescriptor location : locations) {
                String host = location.getDataNodeIP();
                List<TaskInProgress> tasksInHost = cache.get(host);
                if (tasksInHost == null) {
                    tasksInHost = new ArrayList<TaskInProgress>();
                    tasksInHost.add(maps[i]);
                    cache.put(host, tasksInHost);
                }
                //check whether the hostMaps already contains an entry for a TIP
                //This will be true for nodes that are racks and multiple nodes in
                //the rack contain the input for a tip. Note that if it already
                //exists in the hostMaps, it must be the last element there since
                //we process one TIP at a time sequentially in the split-size order
                if (tasksInHost.get(tasksInHost.size() - 1) != maps[i]) {
                    tasksInHost.add(maps[i]);
                }
            }
        }

        return cache;
    }

    public Task obtainNewLocalMapTask(TaskTrackerStatus taskTrackerStatus) {
        return obtainNewMapTask(taskTrackerStatus);
    }

    public Task obtainNewNonLocalMapTask(TaskTrackerStatus taskTrackerStatus) {
        return null;
    }

    public Task obtainNewMapTask(TaskTrackerStatus taskTrackerStatus) {
        int target = findNewMapTask(taskTrackerStatus);
        if (target == -1) {
            return null;
        }

        Task result = maps[target].getTaskToRun(taskTrackerStatus.getTrackerName());

        if (result != null) {
            runningMapTasks++;
        }

        return result;
    }

    private int findNewMapTask(TaskTrackerStatus taskTrackerStatus) {
        if (numMapTasks == 0) {
            System.out.println("No maps to schedule for " + this.jobID);
            return -1;
        }

        String taskTrackerName = taskTrackerStatus.getTrackerName();
        TaskInProgress taskInProgress = null;

        //
        // TODO:
        // Check if too many tasks of this job have failed on this
        // tasktracker prior to assigning it a new one.
        //
        /*
        if (!shouldRunOnTaskTracker(taskTracker)) {
            return -1;
        }
        */

        // When scheduling a map task:
        //  TODO: 0) Schedule a failed task without considering locality
        //  1) Schedule non-running tasks
        //  2) Schedule tasks with no location information

        // 0) Schedule the task with the most failures, unless failure was on this
        //    machine
        /*
        tip = findTaskFromList(failedMaps, tts, numUniqueHosts, false);
            if (tip != null) {
                // Add to the running list
                scheduleMap(tip);
                LOG.info("Choosing a failed task " + tip.getTIPId());
                return tip.getIdWithinJob();
            }
        */

        String trackerHost = jobTracker.getHost(taskTrackerStatus.getTrackerName());

        //
        // 1) Non-running TIP :
        //

        if (trackerHost != null) {
            List<TaskInProgress> allTasks = nonRunningMapTasksMap.get(trackerHost);
            for (TaskInProgress task : allTasks) {
                if (!task.isRunning()) {
                    allTasks.remove(task);
                    taskInProgress = task;
                }
            }
        }

        if (taskInProgress != null) {
            scheduleMap(trackerHost, taskInProgress);
            return taskInProgress.getIdWithinJob();
        }

        // TODO: 2. Search non-local tips for a new task
        /*
        tip = findTaskFromList(nonLocalMaps, tts, numUniqueHosts, false);
        if (tip != null) {
            // Add to the running list
            scheduleMap(tip);

            LOG.info("Choosing a non-local task " + tip.getTIPId());
            return tip.getIdWithinJob();
        }
        */

        return -1;
    }

    private synchronized void scheduleMap(String trackerHost, TaskInProgress taskInProgress) {
        Set<TaskInProgress> hostMaps = runningMapTasksMap.get(trackerHost);
        if (hostMaps == null) {
            hostMaps = new LinkedHashSet<TaskInProgress>();
            runningMapTasksMap.put(trackerHost, hostMaps);
        }
        hostMaps.add(taskInProgress);
    }

    public String getJobID() {
        return jobID;
    }

    public void setJobID(String jobID) {
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

    public boolean isComplete() {
        return this.jobStatus.isJobComplete();
    }
}
