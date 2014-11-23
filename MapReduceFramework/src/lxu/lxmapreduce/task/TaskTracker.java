package lxu.lxmapreduce.task;

import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.job.IJobTracker;
import lxu.lxmapreduce.metadata.*;
import lxu.lxmapreduce.task.map.MapTaskStatus;
import lxu.lxmapreduce.task.reduce.ReduceTaskStatus;
import lxu.lxmapreduce.configuration.Configuration;
import lxu.lxmapreduce.configuration.JobConf;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * TaskTracker.java
 * Created by Wei on 11/12/14.
 *
 * This is the main class for TaskTracker. TaskTracker periodically send
 * heartbeat message to {@link lxu.lxmapreduce.job.JobTracker} and receive
 * actions from JobTracker. Then launch tasks and report their status back
 * to JobTracker.
 */
public class TaskTracker implements Runnable {
	private boolean initialContact;
	private String taskTrackerName;
	private TaskTrackerStatus status;
	private boolean isRunning;
	private boolean acceptNewTasks;
	private int maxMapTasks;
	private int maxReduceTasks;
	private JobConf jobConf;
	private Map<TaskAttemptID, TaskRunner> taskPool;
	private short responseID;           // Last response ID received from JobTracker.
	private IJobTracker jobTrackerService;
	private long lastHeartbeat;
	private long heartbeatInterval;
	private static int MAX_ATTEMPT_NUM = 4;
	private IntermediateListener interListener;

	public TaskTracker(JobConf jobConf,
	                   String taskTrackerName,
	                   int maxMapTasks,
	                   int maxReduceTasks) {
		this.taskTrackerName = taskTrackerName;
		this.maxMapTasks = maxMapTasks;
		this.maxReduceTasks = maxReduceTasks;
		this.taskPool = new HashMap<>();
		this.initialContact = true;
		this.heartbeatInterval = 3 * 1000;
		this.acceptNewTasks = true;
        Registry registry = null;
        try {
            Configuration conf = new Configuration();
            String masterAddr = conf.getSocketAddr("master.address", "localhost");
            int rmiPort = conf.getInt("rmi.port", 1099);
            registry = LocateRegistry.getRegistry(masterAddr, rmiPort);
            this.jobTrackerService = (IJobTracker) registry.lookup("JobTracker");
        } catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
		this.interListener = new IntermediateListener();
        this.isRunning = true;
	}

	public void startInterListener() {
		new Thread(this.interListener).start();
	}

	public static void main(String[] args) {
		JobConf jobConf = null;
		String taskTrackerName =  "taskTracker-" +
				(Math.abs((System.currentTimeMillis() + "").hashCode()%1000)) + "";
		TaskTracker taskTracker = new TaskTracker(jobConf, taskTrackerName, 4, 4);
		new Thread(taskTracker).start();
		taskTracker.startInterListener();
	}

	public TaskTrackerStatus getStatus() {
		return status;
	}

	public void setStatus(TaskTrackerStatus status) {
		this.status = status;
	}

    /**
     * sendHeartBeat
     *
     * Collect the status of all tasks and send heartbeat to JobTracker.
     * @return {@link lxu.lxmapreduce.metadata.HeartbeatResponse} from JobTracker.
     * @throws UnknownHostException
     */
	public HeartbeatResponse sendHeartBeat() throws UnknownHostException {
		// Create TaskTrackerStatus.
		TaskTrackerStatus taskTrackerStatus = buildTaskTrackerStatus();

		acceptNewTasks = acceptNewTasks && taskTrackerStatus.hasFreeSlots();

        HeartbeatResponse heartBeatResponse =
                null;
        try {
            heartBeatResponse = this.jobTrackerService.heartbeat(taskTrackerStatus,
                    this.initialContact,
                    acceptNewTasks,
                    responseID);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
	        e.printStackTrace();
        }

		processHeartBeatResponse(heartBeatResponse);

        if (this.initialContact == true) {
            this.initialContact = false;
        }

		return heartBeatResponse;
	}

    /**
     * processHeartBeatResponse
     *
     * Process the response from JobTracker.
     *
     * @param heartBeatResponse
     */
	public void processHeartBeatResponse(HeartbeatResponse heartBeatResponse) {
		// Update last responseID.
		this.responseID = heartBeatResponse.getResponseID();

		// Handle JobTracker commands(TaskTrackerActions)
		for (TaskTrackerAction action : heartBeatResponse.getActions()) {
			processAction(action, heartBeatResponse);
		}

	}

	/**
     * processAction
     *
	 * Process actions received from JobTracker.
     *
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
     * offerService
     *
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
     * launchTask
     *
	 * Launch a task and add it to the taskPool.
	 *
	 * @param jobConf
	 * @param task
	 */
	public void launchTask(JobConf jobConf, Task task) {
		TaskRunner taskRunner = new TaskRunner(jobConf, task);
		this.taskPool.put(task.getTaskAttemptID(), taskRunner);
        taskRunner.start();
	}

	/**
     * buildTaskTrackerStatus
     *
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
        Iterator<TaskAttemptID> attemptIDIterator = this.taskPool.keySet().iterator();
        while (attemptIDIterator.hasNext()) {
            TaskAttemptID taskAttemptID = attemptIDIterator.next();
			TaskRunner taskRunner = this.taskPool.get(taskAttemptID);
			TaskStatus taskStatus = taskRunner.getStatus();

			if (taskRunner.status.getState() == TaskStatus.SUCCEEDED) {
                attemptIDIterator.remove();
				//this.taskPool.remove(taskAttemptID);
			} else { // Terminate the task tracker.
				if (taskRunner.status.getState() == TaskStatus.FAILED) {
					if (taskRunner.getAttempNum() == MAX_ATTEMPT_NUM) {
						System.err.println("Task fails.");
					} else { // Restart this task.
						taskStatus.setState(TaskStatus.RUNNING);
						taskRunner.getStatus().setState(TaskStatus.RUNNING);
						taskRunner.setAttempNum(taskRunner.getAttempNum() + 1);
						new Thread(taskRunner).start();
					}
				}
			}

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
        System.out.println("Task tracker " + this.taskTrackerName + " started!");
		while (this.isRunning) {
			try {
				offerService();
			} catch (InterruptedException |
					UnknownHostException e) {
				e.printStackTrace();
			}
		}
	}

	// Run a map/reduce task in a thread.
	public class TaskRunner extends Thread {
		private TaskStatus status;
		private Task task;
		private JobConf jobConf;
		private int attempNum;

		public TaskRunner(JobConf jobConf, Task task) {
			this.task = task;
			this.jobConf = jobConf;
            this.task.setConf(jobConf);
			this.attempNum = 0;

            if (task.isMapTask()) {
                this.status = new MapTaskStatus(task.getTaskAttemptID(),
                                                taskTrackerName,
                                                TaskStatus.PREP);
            } else {
                this.status = new ReduceTaskStatus(task.getTaskAttemptID(),
                                                   taskTrackerName,
                                                   TaskStatus.PREP);
            }

            task.initialize();
		}

		public int getAttempNum() {
			return attempNum;
		}

		public void setAttempNum(int attempNum) {
			this.attempNum = attempNum;
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
                System.out.println("Task " + task.taskAttemptID.toString() + " is running");
				this.task.run(this.jobConf);
				this.status.setState(TaskStatus.SUCCEEDED);
			} catch (IOException
					| InvocationTargetException
					| NoSuchMethodException
					| ClassNotFoundException
					| InstantiationException
					| IllegalAccessException e) {
				this.status.setState(TaskStatus.FAILED);
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			}
		}
	}

	// A thread that listen for reducers' request for input
	// and send intermediate data to reducer.
	public class IntermediateListener implements Runnable {
		private int port = 19001;           // Listening port
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

    /**
     * SendDataThread
     *
     * Send map output data to reducer.
     */
	public class SendDataThread implements Runnable {
		private Socket sock;
		public SendDataThread (Socket sock) {
			this.sock = sock;
		}

		@Override
		public void run() {
			// Read
			try {
				HashMap<Text, LinkedList<Text>> map = null;

				ObjectInputStream in = new ObjectInputStream(sock.getInputStream());
				TaskAttemptID taskID = (TaskAttemptID) in.readObject();
                System.err.println("Read reducer id " + taskID.getTaskID());
				map = getReduceInput(taskID);
				ObjectOutput out = new ObjectOutputStream(sock.getOutputStream());
				out.writeObject(map);

				System.err.println("Send data to reducer");

				in.close();
				out.close();
				sock.close();
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

		private HashMap<Text, LinkedList<Text>> getReduceInput(TaskAttemptID taskID) {
            File folder = new File("mapoutput/");
            String namePrefix = taskID.getTaskID().toString();
            HashMap<Text, LinkedList<Text>> contents = new HashMap<Text, LinkedList<Text>>();
            for (File fileEntry : folder.listFiles()) {
                if (fileEntry.isFile() && fileEntry.getName().startsWith(namePrefix)) {
                    try {
                        BufferedReader reader = new BufferedReader(new FileReader(fileEntry));
                        String line = null;
                        while ((line = reader.readLine()) != null) {
                            String[] info = line.split("\t");
                            Text key = new Text(info[0]);
                            Text value = new Text(info[1]);
                            LinkedList<Text> values = contents.get(key);
                            if (values == null) {
                                values = new LinkedList<Text>();
                                contents.put(key, values);
                            }
                            values.add(value);

                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return contents;
		}
	}
}
