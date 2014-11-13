package lxu.lxmapreduce.task;

import lxu.lxmapreduce.metadata.HeartbeatResponse;
import lxu.lxmapreduce.metadata.TaskTrackerStatus;
import lxu.lxmapreduce.tmp.JobConf;
import lxu.lxmapreduce.tmp.TaskID;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Wei on 11/12/14.
 */
public class TaskTracker implements Runnable {
	private String taskTrackerName;
	private TaskTrackerStatus status;
	private boolean isRunning;
	private Map<TaskID, TaskInProgress> tasks;
	private int maxMapTasks;
	private int maxReduceTasks;
	private JobConf jobConf;
	private Map<TaskID, TaskRunner> taskPool;
	private short responseID;           // Last response ID received from JobTracker.

	public TaskTracker(JobConf jobConf,
	                   String taskTrackerName,
	                   int maxMapTasks,
	                   int maxReduceTasks) {
		this.taskTrackerName = taskTrackerName;
		this.tasks = new HashMap<TaskID, TaskInProgress>();
		this.maxMapTasks = maxMapTasks;
		this.maxReduceTasks = maxReduceTasks;
		this.taskPool = new HashMap<TaskID, TaskRunner>();
	}

	public static void main(String[] args) {
		JobConf jobConf = null;
		TaskTracker taskTracker = new TaskTracker(jobConf, "TaskTracker1", 4, 4);

	}

	public TaskTrackerStatus getStatus() {
		return status;
	}

	public void setStatus(TaskTrackerStatus status) {
		this.status = status;
	}

	public HeartbeatResponse sendHeartBeat() throws UnknownHostException {

		// Create TaskTrackerStatus.
		TaskTrackerStatus taskTrackerStatus = buildTaskTrackerStatus();

		boolean isFree = taskTrackerStatus.hasFreeSlots();


		//taskTrackerStatus || justStarted || justInited || isFree || heartbeatResponseId
		//HeartbeatResponse heartbeatResponse = jobClient.sendHeartBeat();

		return null;
	}

	public void processHeartBeatResponse(HeartbeatResponse heartBeatResponse) {
		// Update last responseID.
		this.responseID = heartBeatResponse.getResponseID();

		// Handle JobTracker commands(TaskTrackerActions)
		for (TaskTrackerAction action : heartBeatResponse.getActions()) {
			processAction(action, heartBeatResponse);
		}

	}
	public void processAction(TaskTrackerAction action, HeartbeatResponse heartBeatResponse) {
		Task task = null;

		if (action instanceof LaunchAction) {
			JobConf jobConf = new JobConf(heartBeatResponse.getConfiguration());

			if () {
				task = new MapTask();
			} else {
				task = new ReduceTask();
			}

			launchTask(jobConf, task);
		} else if (action instanceof) {

		} else if (action instanceof) {

		} else if (action instanceof) {

		}
	}

	/**
	 * Launch  a task and add it to the taskPoll.
	 * @param jobCOnf
	 * @param task
	 */
	public void launchTask(JobConf jobCOnf, Task task) {
		TaskRunner taskRunner = new TaskRunner(jobConf, task);
		this.taskPool.add(taskRunner);
	}

	/**
	 * Build taskTrackerStatus for heart beat.
	 *
	 * @return
	 */
	public TaskTrackerStatus buildTaskTrackerStatus() throws UnknownHostException {

		return new TaskTrackerStatus(
				this.taskTrackerName,
				InetAddress.getLocalHost().getHostAddress(),
				0,
				this.maxMapTasks,
				this.maxReduceTasks
		);
	}

	/**
	 * A thread that sends heart beat to JobTracker.
	 */
	@Override
	public void run() {
		while (this.isRunning) {
			try {
				HeartbeatResponse heartbeatResponse = sendHeartBeat();

			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
	}

	static enum State {NORMAL, INTERRUPTED}

	// Run a map/reduce task in a thread.
	public class TaskRunner implements Runnable {
		private TaskStatus status;
		private Task task;
		private JobConf jobConf;

		public TaskRunner(JobConf jobConf, Task task) {
			this.task = task;
			this.jobConf = jobConf;
		}

		public Task getTask() {
			return task;
		}

		public void setTask(Task task) {
			this.task = task;
		}

		public TaskStatus getStatus() {
			return status;
		}

		public void setStatus(TaskStatus status) {
			this.status = status;
		}

		/**
		 * Start a map/reduce task.
		 */
		@Override
		public void run() {
			try {
				this.task.run(this.jobConf);
			} catch (IOException
					| InvocationTargetException
					| NoSuchMethodException
					| ClassNotFoundException
					| InstantiationException
					| IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}
}
