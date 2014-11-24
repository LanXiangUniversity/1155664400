package lxu.lxmapreduce.job;

import lxu.lxdfs.service.INameSystemService;
import lxu.lxmapreduce.metadata.*;
import lxu.lxmapreduce.task.*;
import lxu.lxmapreduce.configuration.Configuration;
import lxu.lxmapreduce.configuration.JobConf;
import lxu.lxmapreduce.task.TaskID;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JobTracker.java
 * Created by magl on 14/11/10.
 *
 * This class is the main class to manage all jobs. It maintains all crucial
 * data structures of all running jobs and tasks.
 */
public class JobTracker implements IJobTracker {
    private static int HEARTBEAT_TIMEOUT =
            (new Configuration().getInt("mapreduce.heatbeat.interval", 10 * 1000));
	// All known jobs (jobID -> JobInProgress)
	public ConcurrentHashMap<String, JobInProgress> jobs;
	private int nextJobID = 0;
	private TaskScheduler taskScheduler = null;
	private JobConf jobConf;
    private INameSystemService nameNode = null;
    private TaskTrackerTimeoutListener taskTrackerTimeoutListener = null;
	// All known tasks (taskAttemptID -> TaskInProgress)
	private ConcurrentHashMap<TaskID, TaskInProgress> taskIDToTIPMap;

	// (taskAttemptID -> TaskTrackerName)
	private ConcurrentHashMap<TaskID, String> taskIDToTrackerMap;

	// (TaskTrackerName -> Set<running tasks>)
	private ConcurrentHashMap<String, Set<TaskID>> taskTrackerToTaskmap;

	// (TaskTrackerName -> Set<completed tasks>)
	private ConcurrentHashMap<String, Set<TaskID>> taskTrackerToCompleteTaskMap;

	// (TaskTrackerName -> HostIP)
	private ConcurrentHashMap<String, String> taskTrackerToHostIPMap;

	// (TaskTrackerName -> TaskTrackerStatus)
	private ConcurrentHashMap<String, TaskTrackerStatus> taskTrackers;

	public JobTracker(INameSystemService nameNode) throws RemoteException, NotBoundException {
        //this.jobConf = new JobConf(new Configuration());
		this.jobs = new ConcurrentHashMap<String, JobInProgress>();
		this.taskIDToTIPMap = new ConcurrentHashMap<TaskID, TaskInProgress>();
		this.taskIDToTrackerMap = new ConcurrentHashMap<TaskID, String>();
		this.taskTrackerToTaskmap = new ConcurrentHashMap<String, Set<TaskID>>();
		this.taskTrackerToCompleteTaskMap = new ConcurrentHashMap<String, Set<TaskID>>();
		this.taskTrackerToHostIPMap = new ConcurrentHashMap<String, String>();
		this.taskTrackers = new ConcurrentHashMap<String, TaskTrackerStatus>();
		this.taskScheduler = new TaskScheduler();

        taskTrackerTimeoutListener = new TaskTrackerTimeoutListener();
        (new Thread(taskTrackerTimeoutListener)).start();
        this.nameNode = nameNode;
	}

    /**
     * getNewJobID
     *
     * Assign a job id for new job.
     *
     * @return The new job id.
     */
	@Override
	public String getNewJobID() {
		return "job_" + this.nextJobID++;
	}

    /**
     * submitJob
     *
     * The service for client to submit a new job.
     *
     * @param jobID
     * @param jobConf
     * @return
     */
	@Override
	public JobStatus submitJob(String jobID, JobConf jobConf) {
        this.jobConf = jobConf;
		if (jobs.containsKey(jobID)) {
			return jobs.get(jobID).getJobStatus();
		}

		JobInProgress job = new JobInProgress(jobID, jobConf, this);

		jobs.put(jobID, job);

		job.initTasks();

        taskScheduler.addJobToQueue(jobID);

		return job.getJobStatus();
	}

    /**
     * getJobStatus
     *
     * The service for client to update a job status.
     *
     * @param jobID
     * @return
     */
    @Override
    public JobStatus getJobStatus(String jobID) {
        JobInProgress jobInProgress = jobs.get(jobID);
        return jobInProgress.getJobStatus();
    }

    public String getHost(String taskTrackerName) {
        return taskTrackerToHostIPMap.get(taskTrackerName);
    }

    public JobInProgress getJobInProgress(String jobID) {
        return this.jobs.get(jobID);
    }

    /**
     * heartbeat
     *
     * The service for {@link lxu.lxmapreduce.task.TaskTracker} to report the statuses of
     * all running tasks and itself.
     *
     * @param status
     * @param initialContact
     * @param acceptNewTasks
     * @param responseID
     * @return
     * @throws RemoteException
     * @throws NotBoundException
     */
	@Override
	public HeartbeatResponse heartbeat(TaskTrackerStatus status,
	                                   boolean initialContact,
	                                   boolean acceptNewTasks,
	                                   short responseID) throws RemoteException, NotBoundException {
		String trackerName = status.getTrackerName();
		long now = System.currentTimeMillis();

		short newResponseID = (short) (responseID + 1);

		status.setLastSeen(now);
		// Update TaskTracker, Job, Task information
        if (initialContact) {
            taskTrackerToHostIPMap.put(trackerName, status.getHostIP());
        }
		taskTrackers.put(trackerName, status);
		updateTaskStatuses(status);

        // response, contains new tasks to be run
		HeartbeatResponse heartbeatResponse =
				new HeartbeatResponse(newResponseID, jobConf, null);
		ArrayList<TaskTrackerAction> actions = new ArrayList<TaskTrackerAction>();

        if (jobs.size() != 0) {
            if (acceptNewTasks) {
                List<Task> tasks = taskScheduler.assignTasks(taskTrackers.get(trackerName));
                if (tasks != null) {
                    for (Task task : tasks) {
                        actions.add(new LaunchTaskAction(task));
                    }
                }
            }
        }

        heartbeatResponse.setActions(actions);

		return heartbeatResponse;
	}

	@Override
	public ConcurrentHashMap<String, JobInProgress> getJobs() throws RemoteException {
		return this.jobs;
	}

	/**
     * updateTaskStatuses
     *
     * Update the statuses of all tasks running on one TaskTracker.
     *
     * @param status
     */
	public void updateTaskStatuses(TaskTrackerStatus status) {
		String trackerName = status.getTrackerName();
        LinkedList<TaskStatus> taskReports = status.getTaskReports();

        if (taskReports == null || taskReports.size() == 0) {
            return;
        }

		for (TaskStatus report : taskReports) {
			report.setTaskTracker(trackerName);
			TaskAttemptID taskID = report.getTaskID();

			JobInProgress job = jobs.get(taskID.getJobID());
			TaskInProgress taskInProgress = taskIDToTIPMap.get(taskID.getTaskID());
			job.updateTaskStatus(taskInProgress, report);

			if (job.isComplete()) {
				System.out.println("Job " + job.getJobID() + " Completed");
			}
		}
	}

    /**
     * createTaskEntry
     *
     * Record useful information for the task.
     *
     * @param attemptID
     * @param taskTrackerName
     * @param taskInProgress
     */
    public void createTaskEntry(TaskAttemptID attemptID,
                                String taskTrackerName,
                                TaskInProgress taskInProgress) {
        taskIDToTrackerMap.put(attemptID.getTaskID(), taskTrackerName);

        Set<TaskID> taskSet = taskTrackerToTaskmap.get(taskTrackerName);
        if (taskSet == null) {
            taskSet = new HashSet<TaskID>();
            taskTrackerToTaskmap.put(taskTrackerName, taskSet);
        }
        taskSet.add(attemptID.getTaskID());

        taskIDToTIPMap.put(attemptID.getTaskID(), taskInProgress);
    }

    /**
     * addSucceedTask
     *
     * If one task succeeded, record it.
     *
     * @param attemptID
     * @param taskTrackerName
     */
    public void addSucceedTask(TaskAttemptID attemptID, String taskTrackerName) {
        Set<TaskID> runningTasks = taskTrackerToTaskmap.get(taskTrackerName);
        if (runningTasks == null || runningTasks.size() == 0) {
            System.err.println("Error: Adding wrong succeed task " +
                               attemptID.getTaskID());
            return;
        }
        runningTasks.remove(attemptID.getTaskID());

        Set<TaskID> completedTasks = taskTrackerToCompleteTaskMap.get(taskTrackerName);
        if (completedTasks == null) {
            completedTasks = new HashSet<TaskID>();
            taskTrackerToCompleteTaskMap.put(taskTrackerName, completedTasks);
        }
        completedTasks.add(attemptID.getTaskID());
    }

    /**
     * startService
     *
     * JobTracker register its service to java rmiregistry
     *
     * @param registry
     * @param rmiPort
     */
	public void startService(Registry registry, int rmiPort) {
		// set job tracker of task scheduler
		this.taskScheduler.setJobTracker(this);

		try {
            Configuration configuration = new Configuration();

			IJobTracker stub =
					(IJobTracker) UnicastRemoteObject.exportObject(this, rmiPort);
			registry.rebind("JobTracker", stub);
            System.out.println("JobTracker Started");
		} catch (RemoteException e) {
			System.err.println("Error: JobTracker start RMI service error");
            e.printStackTrace();
        }
    }

    public INameSystemService getNameNode() {
        return this.nameNode;
    }

    public String getTaskLocation(TaskID taskID) {
        String trackerName = taskIDToTrackerMap.get(taskID);
        String hostIP = taskTrackerToHostIPMap.get(trackerName);
        return hostIP;
    }

    /**
     * TaskTrackerTimeoutListener
     *
     * A class periodically check whether there exists some {@link lxu.lxmapreduce.task.TaskTracker}
     * if out of service.
     */
    class TaskTrackerTimeoutListener implements Runnable {
        @Override
        public void run() {
            System.out.println("TaskTrackerTimeoutListener started!");

            while (true) {
                try {
                    long currentTime = System.currentTimeMillis();

                    // Check every task tracker
                    for (Map.Entry<String, TaskTrackerStatus> entry :
                            taskTrackers.entrySet()) {
                        String trackerName = entry.getKey();
                        TaskTrackerStatus status = entry.getValue();
                        long lastSeen = status.getLastSeen();

                        if ((currentTime - lastSeen) > HEARTBEAT_TIMEOUT) {
                            System.out.println("TaskTracker " + trackerName + " failed!");
                            // TaskTracker is out of service
                            // set all completed tasks failed
                            Set<TaskID> completedTasks =
                                    taskTrackerToCompleteTaskMap.get(trackerName);
                            if (completedTasks != null && completedTasks.size() != 0) {
                                taskTrackerToCompleteTaskMap.remove(trackerName);
                                for (TaskID completedTask : completedTasks) {
                                    String jobID = completedTask.getJobID();
                                    JobInProgress jobInProgress = jobs.get(jobID);
                                    if (jobInProgress.isComplete()) {
                                        continue;
                                    }
                                    System.out.println("Set completed task " +
                                            completedTask + " to failed");
                                    TaskInProgress task = taskIDToTIPMap.get(completedTask);
                                    jobInProgress.addFailedTaskSet(task, true);
                                    taskIDToTrackerMap.remove(completedTask);
                                    taskIDToTIPMap.remove(completedTask);
                                }
                            }

                            // set all running tasks failed
                            Set<TaskID> runningTasks = taskTrackerToTaskmap.get(trackerName);
                            if (runningTasks != null && runningTasks.size() != 0) {
                                taskTrackerToTaskmap.remove(trackerName);
                                for (TaskID runningTask : runningTasks) {
                                    String jobID = runningTask.getJobID();
                                    JobInProgress jobInProgress = jobs.get(jobID);

                                    System.out.println("Set completed task " +
                                            runningTask + " to failed");

                                    TaskInProgress task = taskIDToTIPMap.get(runningTask);
                                    jobInProgress.addFailedTaskSet(task, false);
                                    taskIDToTrackerMap.remove(runningTask);
                                    taskIDToTIPMap.remove(runningTask);
                                }
                            }

                            // remove the record of the task tracker
                            taskTrackerToHostIPMap.remove(trackerName);
                            taskTrackers.remove(trackerName);
                        }
                    }
                    Thread.sleep(HEARTBEAT_TIMEOUT);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
