package lxu.lxmapreduce.job;

import lxu.lxmapreduce.metadata.HeartbeatResponse;
import lxu.lxmapreduce.metadata.TaskTrackerAction;
import lxu.lxmapreduce.metadata.TaskTrackerStatus;
import lxu.lxmapreduce.task.*;
import lxu.lxmapreduce.tmp.Configuration;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

/**
 * Created by magl on 14/11/10.
 */
public class JobTracker implements IJobTracker {
	// All known jobs (jobID -> JobInProgress)
	public HashMap<String, JobInProgress> jobs;
	private int nextJobID = 0;
	private TaskScheduler taskScheduler = null;
	private Configuration jobConf;
	// All known tasks (taskAttemptID -> TaskInProgress)
	private HashMap<TaskAttemptID, TaskInProgress> taskIDToTIPMap;

	// (taskAttemptID -> TaskTrackerName)
	private HashMap<TaskAttemptID, String> taskIDToTrackerMap;

	// (TaskTrackerName -> Set<running tasks>)
	private HashMap<String, Set<TaskAttemptID>> taskTrackerToTaskmap;

	// (TaskTrackerName -> Set<completed tasks>)
	private HashMap<String, Set<TaskAttemptID>> taskTrackerToCompleteTaskMap;

	// (TaskTrackerName -> HostIP)
	private HashMap<String, String> taskTrackerToHostIPMap;

	// (TaskTrackerName -> TaskTrackerStatus)
	private HashMap<String, TaskTrackerStatus> taskTrackers;

	public JobTracker() {
		this.jobs = new HashMap<String, JobInProgress>();
		this.taskIDToTIPMap = new HashMap<TaskAttemptID, TaskInProgress>();
		this.taskIDToTrackerMap = new HashMap<TaskAttemptID, String>();
		this.taskTrackerToTaskmap = new HashMap<String, Set<TaskAttemptID>>();
		this.taskTrackerToCompleteTaskMap = new HashMap<String, Set<TaskAttemptID>>();
		this.taskTrackerToHostIPMap = new HashMap<String, String>();
		this.taskTrackers = new HashMap<String, TaskTrackerStatus>();
		this.taskScheduler = new TaskScheduler();
	}

	public static void main(String[] args) {
		JobTracker jobTracker = new JobTracker();
		jobTracker.startService();
	}

	@Override
	public String getNewJobID() {
		return "job_" + this.nextJobID++;
	}

	@Override
	public JobStatus submitJob(String jobID, Configuration jobConf) {
		this.jobConf = jobConf;
		if (jobs.containsKey(jobID)) {
			return jobs.get(jobID).getJobStatus();
		}

		JobInProgress job = new JobInProgress(jobID, this);

		jobs.put(jobID, job);

		// TODO: add job to taskScheduler Listener
		job.initTasks();

		return job.getJobStatus();
	}

    public String getHost(String taskTrackerName) {
        return taskTrackerToHostIPMap.get(taskTrackerName);
    }

	@Override
	public HeartbeatResponse heartbeat(TaskTrackerStatus status,
	                                   boolean initialContact,
	                                   boolean acceptNewTasks,
	                                   short responseID) {
		String trackerName = status.getTrackerName();
		long now = System.currentTimeMillis();

		short newResponseID = (short) (responseID + 1);

		status.setLastSeen(now);
		// Update TaskTracker, Job, Task information
		taskTrackers.put(trackerName, status);
		updateTaskStatuses(status);

		// TODO: Generate HeartbeatResponse (including task tracker action)
		HeartbeatResponse heartbeatResponse =
				new HeartbeatResponse(newResponseID, jobConf, null);
		ArrayList<TaskTrackerAction> actions = new ArrayList<TaskTrackerAction>();

		if (acceptNewTasks) {
			List<Task> tasks = taskScheduler.assignTasks(taskTrackers.get(trackerName));
			if (tasks != null) {
				for (Task task : tasks) {
					// TODO: Build LaunchTaskAction
				}
			}
		}

		return heartbeatResponse;
	}

	public void updateTaskStatuses(TaskTrackerStatus status) {
		String trackerName = status.getTrackerName();

		for (TaskStatus report : status.getTaskReports()) {
			report.setTaskTracker(trackerName);
			String taskID = report.getTaskID();

			JobInProgress job = jobs.get(report.getJobID());
			TaskInProgress taskInProgress = taskIDToTIPMap.get(taskID);
			job.updateTaskStatus(taskInProgress, report);

			if (job.isComplete()) {
				System.out.println("Job " + job.getJobID() + " Completed");
				jobs.remove(report.getJobID());
			}
		}
	}

    public void createTaskEntry(TaskAttemptID attemptID,
                                String taskTrackerName,
                                TaskInProgress taskInProgress) {
        taskIDToTrackerMap.put(attemptID, taskTrackerName);

        Set<TaskAttemptID> taskSet = taskTrackerToTaskmap.get(taskTrackerName);
        if (taskSet == null) {
            taskSet = new HashSet<TaskAttemptID>();
            taskTrackerToTaskmap.put(taskTrackerName, taskSet);
        }
        taskSet.add(attemptID);

        taskIDToTIPMap.put(attemptID, taskInProgress);
    }

	public void startService() {
		// set job tracker of task scheduler
		this.taskScheduler.setJobTracker(this);

		try {
			IJobTracker stub =
					(IJobTracker) UnicastRemoteObject.exportObject(this, 0);
			Registry registry = LocateRegistry.getRegistry();
			registry.rebind("JobTracker", stub);
		} catch (RemoteException e) {
			System.err.println("Error: JobTracker start RMI service error");
			System.err.println(e.getMessage());
		}
	}
}
