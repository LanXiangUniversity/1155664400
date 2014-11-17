package lxu.lxmapreduce.job;

import lxu.lxdfs.metadata.DataNodeDescriptor;
import lxu.lxdfs.metadata.LocatedBlock;
import lxu.lxmapreduce.metadata.TaskTrackerStatus;
import lxu.lxmapreduce.task.*;
import lxu.lxmapreduce.tmp.JobConf;

import java.rmi.RemoteException;
import java.util.*;

/**
 * Created by magl on 14/11/10.
 */
public class JobInProgress {
    private String jobID;
    private JobStatus jobStatus = null;
    private JobConf jobConf = null;
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

    private Set<TaskInProgress> nonRunningReduceTaskSet;
    private Set<TaskInProgress> runningReduceTaskSet;

    private boolean tasksInited = false;

    public JobInProgress(String jobID, JobConf jobConf, JobTracker jobTracker) {
        this.jobID = jobID;
        this.jobConf = jobConf;
        this.jobTracker = jobTracker;
        this.jobStatus = new JobStatus(jobID, JobStatus.PREP, JobStatus.PREP);
        this.numMapTasks = jobConf.getNumMapTasks();
        this.numReduceTasks = jobConf.getNumReduceTasks();
        nonRunningReduceTaskSet = new HashSet<TaskInProgress>();
        runningReduceTaskSet = new HashSet<TaskInProgress>();
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
            nonRunningReduceTaskSet.add(this.reduces[i]);
        }

        tasksInited = true;
        runningMapTasksMap = new HashMap<String, Set<TaskInProgress>>();
    }

    // TODO: connect to namenode to get all blocks of given file
    public LocatedBlock[] getFileBlocks(String fileName) {
        try {
            return jobTracker.getNameNode().getFileBlocks(fileName).getBlocks();
        } catch (RemoteException e) {
            System.out.println("Error: Cannot get file blocks while initializing Tasks!");
            System.out.println(e.getMessage());
            return null;
        }
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
        TaskAttemptID taskID = status.getTaskID();
        System.out.println("Task '" + taskID + "' has completed!");
        tip.setTaskCompleted(taskID);
        if (tip.isMapTask()) {
            runningMapTasks--;
            finishedMapTasks++;
            if (finishedMapTasks == numMapTasks) {
                this.jobStatus.setMapState(JobStatus.SUCCEEDED);
            }

            if (runningMapTasksMap == null) {
                System.err.println("Running cache for map missing!!");
            }
            String hostIP = jobTracker.getHost(status.getTaskTracker());
            Set<TaskInProgress> hostMap = runningMapTasksMap.get(hostIP);
            if (hostMap != null) {
                hostMap.remove(tip);
                if (hostMap.size() == 0) {
                    runningMapTasksMap.remove(hostIP);
                }
            }
        } else {
            runningReduceTasks--;
            finishedReduceTasks++;
            if (finishedReduceTasks == numReduceTasks) {
                this.jobStatus.setReduceState(JobStatus.SUCCEEDED);
            }
            runningReduceTaskSet.remove(tip);
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
                    cache.put(host, tasksInHost);
                }
                tasksInHost.add(maps[i]);
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

    // TODO:
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
            if (jobStatus.getMapState() == JobStatus.PREP) {
                jobStatus.setMapState(JobStatus.RUNNING);
            }
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
                    break;
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

    public Task obtainNewReduceTask(TaskTrackerStatus taskTrackerStatus) {
        if (jobStatus.getMapState() != JobStatus.SUCCEEDED) {
            System.out.println("Error: Cannot assign reduce task before map finishing");
            return null;
        }

        if (!shouldAssignReduceTask()) {
            return null;
        }

        int target = findNewReduceTasks(taskTrackerStatus);

        if (target == -1) {
            return null;
        }

        Task task = reduces[target].getTaskToRun(taskTrackerStatus.getTrackerName());

        if (task != null) {
            runningReduceTasks++;
            if (jobStatus.getReduceState() == JobStatus.PREP) {
                jobStatus.setReduceState(JobStatus.RUNNING);
            }
        }

        return null;
    }

    private int findNewReduceTasks(TaskTrackerStatus taskTrackerStatus) {
        if (numReduceTasks == 0) {
            System.out.println("No reduces to schedule for " + this.jobID);
            return -1;
        }

        String taskTrackerName = taskTrackerStatus.getTrackerName();
        TaskInProgress taskInProgress = null;

        for (TaskInProgress reduceTask : nonRunningReduceTaskSet) {
            if (!reduceTask.isRunning()) {
                taskInProgress = reduceTask;
                nonRunningReduceTaskSet.remove(reduceTask);
            }
        }

        if (taskInProgress != null) {
            scheduleReduce(taskInProgress);
            return taskInProgress.getIdWithinJob();
        }

        return -1;
    }

    private boolean shouldAssignReduceTask() {
        return finishedMapTasks == numMapTasks;
    }

    private void scheduleReduce(TaskInProgress taskInProgress) {
        if (runningReduceTaskSet == null) {
            System.err.println("Running cache for reducers missing!! Job details are missing");
            return;
        }
        runningReduceTaskSet.add(taskInProgress);
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
