package lxu.lxmapreduce.job;

import lxu.lxdfs.service.INameSystemService;
import lxu.lxmapreduce.metadata.*;
import lxu.lxmapreduce.task.*;
import lxu.lxmapreduce.tmp.Configuration;
import lxu.lxmapreduce.tmp.JobConf;
import lxu.lxmapreduce.tmp.TaskID;

import java.rmi.NotBoundException;
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
	private JobConf jobConf;
    private INameSystemService nameNode = null;
	// All known tasks (taskAttemptID -> TaskInProgress)
	private HashMap<TaskID, TaskInProgress> taskIDToTIPMap;

	// (taskAttemptID -> TaskTrackerName)
	private HashMap<TaskID, String> taskIDToTrackerMap;

	// (TaskTrackerName -> Set<running tasks>)
	private HashMap<String, Set<TaskID>> taskTrackerToTaskmap;

	// (TaskTrackerName -> Set<completed tasks>)
	private HashMap<String, Set<TaskID>> taskTrackerToCompleteTaskMap;

	// (TaskTrackerName -> HostIP)
	private HashMap<String, String> taskTrackerToHostIPMap;

	// (TaskTrackerName -> TaskTrackerStatus)
	private HashMap<String, TaskTrackerStatus> taskTrackers;

	public JobTracker() throws RemoteException, NotBoundException {
        //this.jobConf = new JobConf(new Configuration());
		this.jobs = new HashMap<String, JobInProgress>();
		this.taskIDToTIPMap = new HashMap<TaskID, TaskInProgress>();
		this.taskIDToTrackerMap = new HashMap<TaskID, String>();
		this.taskTrackerToTaskmap = new HashMap<String, Set<TaskID>>();
		this.taskTrackerToCompleteTaskMap = new HashMap<String, Set<TaskID>>();
		this.taskTrackerToHostIPMap = new HashMap<String, String>();
		this.taskTrackers = new HashMap<String, TaskTrackerStatus>();
		this.taskScheduler = new TaskScheduler();

        Registry registry = LocateRegistry.getRegistry();
        nameNode = (INameSystemService)registry.lookup("NameSystemService");
	}

	@Override
	public String getNewJobID() {
		return "job_" + this.nextJobID++;
	}

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

	@Override
	public HeartbeatResponse heartbeat(TaskTrackerStatus status,
	                                   boolean initialContact,
	                                   boolean acceptNewTasks,
	                                   short responseID) {
		String trackerName = status.getTrackerName();
        //System.out.println("Received heartbeat from " + trackerName);
		long now = System.currentTimeMillis();

		short newResponseID = (short) (responseID + 1);

		status.setLastSeen(now);
		// Update TaskTracker, Job, Task information
        if (initialContact) {
            taskTrackerToHostIPMap.put(trackerName, status.getHostIP());
        }
		taskTrackers.put(trackerName, status);
		updateTaskStatuses(status);

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
				//jobs.remove(report.getJobID());
			}
		}
	}

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

    public void addSucceedTask(TaskAttemptID attemptID, String taskTrackerName) {
        Set<TaskID> completedTasks = taskTrackerToCompleteTaskMap.get(taskTrackerName);
        if (completedTasks == null) {
            completedTasks = new HashSet<TaskID>();
            taskTrackerToCompleteTaskMap.put(taskTrackerName, completedTasks);
        }
        completedTasks.add(attemptID.getTaskID());
    }

	public void startService() {
		// set job tracker of task scheduler
		this.taskScheduler.setJobTracker(this);

		try {
			IJobTracker stub =
					(IJobTracker) UnicastRemoteObject.exportObject(this, 0);
			Registry registry = LocateRegistry.getRegistry();
			registry.rebind("JobTracker", stub);
            System.out.println("JobTracker Started");
		} catch (RemoteException e) {
			System.err.println("Error: JobTracker start RMI service error");
			System.err.println(e.getMessage());
		}
	}

    public INameSystemService getNameNode() {
        return this.nameNode;
    }

    public static void main(String[] args) {
        try {
            JobTracker jobTracker = new JobTracker();
            jobTracker.startService();
        } catch (Exception e) {
            System.err.println("Error: Starting job tracker! ");
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }

}
