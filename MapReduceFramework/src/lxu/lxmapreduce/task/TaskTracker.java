package lxu.lxmapreduce.task;

import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.job.JobTracker;
import lxu.lxmapreduce.metadata.HeartbeatResponse;
import lxu.lxmapreduce.metadata.LaunchTaskAction;
import lxu.lxmapreduce.metadata.TaskTrackerAction;
import lxu.lxmapreduce.metadata.TaskTrackerStatus;
import lxu.lxmapreduce.tmp.JobConf;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by Wei on 11/12/14.
 */
public class TaskTracker implements Runnable {
	private boolean isInitialized;
	private String taskTrackerName;
	private TaskTrackerStatus status;
	private boolean isRunning;
	private int maxMapTasks;
	private int maxReduceTasks;
	private JobConf jobConf;
	private Map<TaskAttemptID, TaskRunner> taskPool;
	private short responseID;           // Last response ID received from JobTracker.
	private JobTracker jobTrackerService;
	private long lastHeartbeat;
	private long heartbeatInterval;
	private IntermediateListener interListener;

	public TaskTracker(JobConf jobConf,
	                   String taskTrackerName,
	                   int maxMapTasks,
	                   int maxReduceTasks) {
		this.taskTrackerName = taskTrackerName;
		this.maxMapTasks = maxMapTasks;
		this.maxReduceTasks = maxReduceTasks;
		this.taskPool = new HashMap<>();
		this.isInitialized = false;
		this.heartbeatInterval = 3 * 1000;
		// TODO get  JobTracker remote reference.
		this.jobTrackerService = null;
		this.interListener = new IntermediateListener();
	}

	public void startInterListener() {
		// TODO: Init interListener
		//this.interListener.setPort();
		//this.interListener.setFileName();
		new Thread(this.interListener).start();
	}

	public static void main(String[] args) {
		JobConf jobConf = null;
		TaskTracker taskTracker = new TaskTracker(jobConf, "TaskTracker1", 4, 4);
		new Thread(taskTracker).start();
		taskTracker.startInterListener();
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

		boolean acceptNewTasks = taskTrackerStatus.hasFreeSlots();

		HeartbeatResponse heartBeatResponse =
				this.jobTrackerService.heartbeat(taskTrackerStatus,
						this.isInitialized,
						acceptNewTasks,
						responseID);

		processHeartBeatResponse(heartBeatResponse);

		return heartBeatResponse;
	}

	public void processHeartBeatResponse(HeartbeatResponse heartBeatResponse) {
		// Update last responseID.
		this.responseID = heartBeatResponse.getResponseID();

		// Handle JobTracker commands(TaskTrackerActions)
		for (TaskTrackerAction action : heartBeatResponse.getActions()) {
			processAction(action, heartBeatResponse);
		}

	}

	/**
	 * Process actions received from JobTracker.
	 * @param action
	 * @param heartBeatResponse
	 */
	public void processAction(TaskTrackerAction action, HeartbeatResponse heartBeatResponse) {
		if (action instanceof LaunchTaskAction) {
			JobConf jobConf = new JobConf(heartBeatResponse.getConfiguration());

			Task task = ((LaunchTaskAction) action).getTask();

			launchTask(jobConf, task);
		}
	}

	/**
	 * Send heartbeat every hearbeatInterval time.
	 *
	 * @throws InterruptedException
	 * @throws UnknownHostException
	 */
	public void offerService() throws InterruptedException, UnknownHostException {
		this.lastHeartbeat = 0;

		while (this.isRunning) {
			long now = System.currentTimeMillis();

			long waitTime = this.heartbeatInterval - (now - lastHeartbeat);

			if (waitTime > 0) {
				Thread.sleep(waitTime);
			}

			// Send hear beat to JobTracker and handle response.
			HeartbeatResponse heartbeatResponse = sendHeartBeat();

			this.lastHeartbeat = System.currentTimeMillis();
		}
	}

	/**
	 * Launch  a task and add it to the taskPoll.
	 *
	 * @param jobConf
	 * @param task
	 */
	public void launchTask(JobConf jobConf, Task task) {
		TaskRunner taskRunner = new TaskRunner(jobConf, task);
		this.taskPool.put(task.getTaskAttemptID(), taskRunner);
	}

	/**
	 * Get status for each task and build taskTrackerStatus for heart beat.
	 *
	 * @return
	 */
	public TaskTrackerStatus buildTaskTrackerStatus() throws UnknownHostException {

		TaskTrackerStatus taskTrackerStatus = new TaskTrackerStatus(
				this.taskTrackerName,
				InetAddress.getLocalHost().getHostAddress(),
				0,
				this.maxMapTasks,
				this.maxReduceTasks
		);

		// Ask for taskReports.
		LinkedList<TaskStatus> statuses = new LinkedList<TaskStatus>();
		for (TaskAttemptID taskAttemptID : this.taskPool.keySet()) {
			TaskRunner taskRunner = this.taskPool.get(taskAttemptID);

			TaskStatus taskStatus = taskRunner.getStatus();

			statuses.add(taskStatus);
		}
		taskTrackerStatus.setTaskReports(statuses);

		return taskTrackerStatus;
	}

	/**
	 * A thread that sends heart beat to JobTracker.
	 */
	@Override
	public void run() {
		while (this.isRunning) {
			try {
				offerService();
			} catch (InterruptedException |
					UnknownHostException e) {
				e.printStackTrace();
			}
		}
	}

	static enum State {NORMAL, INTERRUPTED}

	static class A {
		int v = 0;
	}

	static class B {
		A a = new A();
	}

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
				this.status.setState(TaskStatus.RUNNING);
				this.task.run(this.jobConf);
				this.status.setState(TaskStatus.SUCCEEDED);
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

	// A thread that listen for reducers' request for input
	// and send intermediate data to reducer.
	public class IntermediateListener implements Runnable {
		private int port;           // Listening port
		private String fileName;   // Path of intermediate files.

		public String getFileName() {
			return fileName;
		}

		public void setFileName(String fileName) {
			this.fileName = fileName;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		@Override
		public void run() {
			try {
				ServerSocket srvSock = new ServerSocket(this.port);

				while (true) {
					Socket sock = srvSock.accept();
					System.err.println("receive request from reducer " + sock.getRemoteSocketAddress());
					SendDataThread sendThread = new SendDataThread(sock);
					System.err.println("start a new thread to send data to reducer");
					new Thread(sendThread).start();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public class SendDataThread implements Runnable {
		private Socket sock;
		public SendDataThread (Socket sock) {
			this.sock = sock;
		}

		@Override
		public void run() {
			// Read
			try {
				HashMap<Text, Iterator<Text>> map = null;

				ObjectInputStream in = new ObjectInputStream(sock.getInputStream());
				System.err.println("Read reducer id");
				TaskAttemptID taskID = (TaskAttemptID) in.readObject();
				map = getReduceInput(taskID);
				ObjectOutput out = new ObjectOutputStream(sock.getOutputStream());
				out.writeObject(map);

				System.err.println("Send data to reducer");

				in.close();
				out.close();
				sock.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

		private HashMap<Text, Iterator<Text>> getReduceInput(TaskAttemptID taskID) {
			// TODO: get input for reducer.
			return null;
		}
	}
}
