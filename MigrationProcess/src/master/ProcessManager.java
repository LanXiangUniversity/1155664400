/**
 * ProcessManager.java
 * @author Tong Wei (twei1), Guoli Ma (guolim)
 *
 * Description: The process manager. It is the main class for master server.
 *              Master server has two threads. The daemonThread is used to
 *              accept registration of slave node. The main thread will read 
 *              user input, finish tasks like printing all working process (PS),
 *              launching a new process on slave node (launch), migrating a
 *              process from one node to another (migrate), stop and remove a
 *              process (remove), and terminate a slave node (terminate).
 */

package master;

import processes.MigratableProcess;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

public class ProcessManager {
	private final String prompt = "> ";
	private final int LISTENPORT = 15640;
	/* key = slave node name, value = SlaveState (see SlaveState.java) */
	private ConcurrentHashMap<String, SlaveState> nameSlaveMap;
	/* key = process ID, value = ProcessState (see ProcessState.java) */
	private ConcurrentHashMap<Integer, ProcessState> processMap;
	private int nextPID;
	private RegisterDaemon daemon;
	/* daemon thread to accept the registration of slave node */
	private Thread daemonThread;

	/* Constructor for ProcessManager */
	public ProcessManager() {
		nameSlaveMap = new ConcurrentHashMap<String, SlaveState>();
		processMap = new ConcurrentHashMap<Integer, ProcessState>();
		nextPID = 1;
        /* Start daemon thread */
		daemon = new RegisterDaemon();
		daemonThread = new Thread(daemon);
		daemonThread.start();
	}

	/* usage - Print usage */
	public static void usage() {
		System.out.println();
		System.out.println("USAGE:");
		System.out.println();
		System.out.println("ps" +
				"\t\t\t\t\tPrint all running processes on slaves");
		System.out.println("launch <nodeName> <processName> <args>" +
				"\tlaunch a new process on slave");
		System.out.println("migrate <PID> <toSlaveName>" +
				"\t\tMigrate a process");
		System.out.println("remove <PID>" +
				"\t\t\t\tStop and remove a process");
		System.out.println("terminate <nodeName>" +
				"\t\t\tTerminate a slave node");
		System.out.println("exit" +
				"\t\t\t\t\tExit master.");
	}

	/**
	 * main - main routine of master server. Get user input, parse it, and do
	 * the job.
	 */
	public static void main(String[] args) {
    	/* java ProcessManager */
		if (args.length > 0) {
			usage();
			System.exit(1);
		}

		ProcessManager manager = new ProcessManager();
		Scanner scanner = new Scanner(System.in);
		usage();

		while (true) {
			String[] userCmd = manager.getInput(scanner);

			if (userCmd == null || userCmd[0].equals("exit")) {
				System.out.println();
				break;

			} else if (userCmd[0].equals("launch")) {
				manager.launch(userCmd[1],
						userCmd[2],
						Arrays.copyOfRange(userCmd, 3, userCmd.length));

			} else if (userCmd[0].equals("migrate")) {
				manager.migrate(Integer.parseInt(userCmd[1]), userCmd[2]);

			} else if (userCmd[0].equals("remove")) {
				manager.remove(Integer.parseInt(userCmd[1]));

			} else if (userCmd[0].equals("terminate")) {
				manager.terminate(userCmd[1]);

			} else if (userCmd[0].equals("ps")) {
				manager.ps();
			}
		}
		scanner.close();
		manager.exit();
		System.exit(0);
	}

	/* isSlaveNodeValid - Test whether the nodeName is a registered node */
	private boolean isSlaveNameValid(String nodeName) {
		for (String name : nameSlaveMap.keySet()) {
			if (nodeName.equals(name)) {
            	/* nodeName in the map */
				return true;
			}
		}
		return false;
	}

	/* isPIDValid - Test whether the process id is a running process */
	private boolean isPIDValid(int pid) {
		for (int i : processMap.keySet()) {
			if (pid == i) {
				return true;
			}
		}
		return false;
	}

	/**
	 * getInput - Read user input, parse the input to make sure it is a valid
	 * command.
	 */
	public String[] getInput(Scanner scanner) {
		String[] userCmd = null;

		while (true) {
			System.out.print(prompt);

			if (scanner.hasNextLine()) {
            	/* Not EOF */
				String input = scanner.nextLine();
				userCmd = input.split(" ");

				/**
				 * Test whether the command is *launch, migrate, remove,
				 * terminate, ps, or exit*
				 */

				if (userCmd[0].equals("launch")) {
					if (userCmd.length < 3) {
						continue;
					}
					String nodeName = userCmd[1];
					if (isSlaveNameValid(nodeName)) {
                    	/* slave name is valid */
						break;
					}
					System.out.println(nodeName + " is not a valid slave name");

				} else if (userCmd[0].equals("terminate")) {
					if (userCmd.length != 2) {
						continue;
					}
					String nodeName = userCmd[1];
					if (isSlaveNameValid(nodeName)) {
                    	/* slave name is valid */
						break;
					}
					System.out.println(nodeName + " is not a valid slave name");
				} else if (userCmd[0].equals("migrate")) {
                	/* For migrate, the pid and node name should be valid */

					if (userCmd.length != 3) {
						continue;
					}
					int pid = 0;
					try {
						pid = Integer.parseInt(userCmd[1]);
					} catch (NumberFormatException e) {
						System.out.println(userCmd[1] + " is not parsable");
						continue;
					}

					String nodeName = userCmd[2];

					if (isPIDValid(pid) && isSlaveNameValid(nodeName)) {
						break;
					}
					System.out.println(pid + " is not a valid PID or " +
							nodeName + " is not a valid slave name");

				} else if (userCmd[0].equals("remove")) {
                	/* For remove, the pid should be valid */

					if (userCmd.length != 2) {
						continue;
					}
					int pid = 0;
					try {
						pid = Integer.parseInt(userCmd[1]);
					} catch (NumberFormatException e) {
						System.out.println("Input PID is not parsable");
						continue;
					}
					if (isPIDValid(pid)) {
						break;
					}
					System.out.println(pid + " is not a valid PID");

				} else if (userCmd[0].equals("ps") || userCmd[0].equals("exit")) {
                	/* For ps or exit, they do not need arguments */
					break;

				} else {
                	/* Other input */
					System.out.println(userCmd[0] + " is not a valid command");
				}
			} else {
            	/* User input Ctrl-d */
				return null;
			}
		}
		return userCmd;
	}

	/* launch - Launch a given process on a given slave node */
	public void launch(String nodeName, String processName, String[] args) {
		SlaveState node = nameSlaveMap.get(nodeName);
        
        /* Test whether the connection to the node is OK */
		if (!statusTest(nodeName)) {
			return;
		}
        
        /* Build up command and send it to slave node */
		String command = "launch processes." + processName + " " + nextPID;
		for (String arg : args) {
        	/* Append the args for process constructor */
			command += (" " + arg);
		}
		System.out.print("launching " + processName +
				" on " + nodeName + "...");

		try {
			ObjectOutputStream output = node.getOOS();
			ObjectInputStream input = node.getOIS();

        	/* Send command */
			output.writeObject(command);
        	/* Receive slave response */
			String response = (String) input.readObject();
			if (response.equals("OK")) {
            	/* Slave successfully launch the process */
				System.out.println("done!");
				System.out.println("Process " + processName +
						" is launched on node " + nodeName);
                /* Record all running process */
				processMap.put(nextPID,
						new ProcessState(nextPID, processName, nodeName));
                /* Record what is running on the node */
				nextPID++;
			} else {
            	/* Slave failed to launch the process, print error message */
				System.out.println("failed: " + response);
			}
		} catch (IOException e) {
			System.out.println("error: error sending launching command " +
					"or error receiving slave response");
		} catch (ClassNotFoundException e) {
			// ignore
		}
	}

	/* migrate - Migrate a given process to a given slave node */
	public void migrate(int pid, String toNodeName) {
    	/* Get target process information */
		ProcessState ps = processMap.get(pid);
		String fromNodeName = ps.getNodeName();
        
        /* Test whether the connections to both nodes are OK */
		if (!statusTest(fromNodeName) || !statusTest(toNodeName)) {
			return;
		}

        /* Get slave node information */
		SlaveState fromNode = nameSlaveMap.get(fromNodeName);
		SlaveState toNode = nameSlaveMap.get(toNodeName);
        
        /* If the given process is running on the given slave node */
		if (fromNodeName.equals(toNodeName)) {
			System.out.println("Job " + pid + " is running on " + fromNodeName);
			return;
		}
        
        /* Build up command and send it to slave */
		String command = "migrate " + pid;
		System.out.print("migrating...");
		try {
			ObjectOutputStream fromNodeOutput = fromNode.getOOS();
			ObjectInputStream fromNodeInput = fromNode.getOIS();
            
            /* Send command */
			fromNodeOutput.writeObject(command);
            /* Receive target process */
			MigratableProcess process =
					(MigratableProcess) fromNodeInput.readObject();
            /* Receive slave response */
			String response = (String) fromNodeInput.readObject();

			if (response.equals("OK")) {
            	/* Successfully received the target process */

				processMap.remove(new Integer(pid));
                
                /* Build up command to send target process */
				command = "resume " + pid;

				ObjectInputStream toNodeInput = toNode.getOIS();
				ObjectOutputStream toNodeOutput = toNode.getOOS();
                
                /* Send command */
				toNodeOutput.writeObject(command);
                /* Send target process */
				toNodeOutput.writeObject(process);
                /* Receive slave response */
				response = (String) toNodeInput.readObject();
				if (response.equals("OK")) {
                	/* Migration succeeded */
					System.out.println("done!");
					System.out.println("process " + pid +
							" successfully migrated to " +
							toNodeName);
				}
                
                /* Add target process to map */
				ps.setNodeName(toNodeName);
				this.processMap.put(new Integer(pid), ps);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/* remove - Stop a running process and remove it on the slave node */
	public void remove(int pid) {
    	/* Get running process information */
		ProcessState ps = this.processMap.get(pid);
		String nodeName = ps.getNodeName();
        
        /* Test whether the connection to the node is OK */
		if (!statusTest(nodeName)) {
			return;
		}

		SlaveState node = this.nameSlaveMap.get(nodeName);
		ObjectInputStream nodeInput = node.getOIS();
		ObjectOutputStream nodeOutput = node.getOOS();

		try {
        	/* Build up command */
			System.out.print("removing...");
			String command = "remove " + pid;
        	/* Send command */
			nodeOutput.writeObject(command);
            /* Receive response */
			String response = (String) nodeInput.readObject();
			if (response.equals("OK")) {
            	/* remove succeeded */
				System.out.println("done!");
				System.out.println("process " + pid + " removed successfully!");
				processMap.remove(pid);
			}
		} catch (IOException e) {
			System.out.println("IOException: writing remove command wrong!");
		} catch (ClassNotFoundException e) {
			// ignore
		}
	}

	public void terminate(String nodeName) {
    	/* Test whether the connection to the node is OK */
		if (!statusTest(nodeName)) {
			return;
		}

        /* Get slave node information */
		SlaveState node = this.nameSlaveMap.get(nodeName);
		ObjectInputStream nodeInput = node.getOIS();
		ObjectOutputStream nodeOutput = node.getOOS();

		try {
        	/* Send command */
			nodeOutput.writeObject("terminate");
            /* Receive response */
			String response = (String) nodeInput.readObject();
			if (response.equals("OK")) {
            	/* terminate succeeded */
				System.out.println(nodeName + " terminates successfully!");
                /* Remove all process that is running on the node */
				for (Map.Entry<Integer, ProcessState> entry :
						processMap.entrySet()) {
					int pid = entry.getKey();
					ProcessState ps = entry.getValue();
					if (nodeName.equals(ps.getNodeName())) {
						this.processMap.remove(pid);
					}
				}
				this.nameSlaveMap.remove(nodeName);
				node.deleteSlave();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/* ps - Print all running processes */
	public void ps() {
		System.out.println("All running processes:\n");
        
        /* For each slave node */
		for (Map.Entry<String, SlaveState> entry : nameSlaveMap.entrySet()) {
			String nodeName = entry.getKey();
            
            /* Test whether the connection to the node is OK */
			if (!statusTest(nodeName)) {
				continue;
			}

			SlaveState node = entry.getValue();
			ObjectInputStream nodeInput = node.getOIS();
			ObjectOutputStream nodeOutput = node.getOOS();

			try {
				nodeOutput.writeObject("ps");
                
                /* Receive pid of all finished process on the slave*/
				String terminatedPID = (String) nodeInput.readObject();
				if (!"".equals(terminatedPID)) {
					String[] pidList = terminatedPID.split(" ");
	
	                /* Update the process list on master */
					for (String pid : pidList) {
						this.processMap.remove(new Integer(pid));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
		}
        /* Print the process information */
		System.out.format("\t%5s\t%10s\t%10s\n",
				"PID",
				"ProcessName",
				"NodeName");
		for (Map.Entry<Integer, ProcessState> entry :
				processMap.entrySet()) {
			int pid = entry.getKey();
			ProcessState ps = entry.getValue();
			System.out.format("\t%5d\t%10s\t%10s\n",
					pid,
					ps.getProcessName(),
					ps.getNodeName());
		}
	}

	/* exit - Exit the main thread of ProcessManager and terminate all slaves */
	public void exit() {
		for (Map.Entry<String, SlaveState> entry : nameSlaveMap.entrySet()) {
			String nodeName = entry.getKey();
			SlaveState node = entry.getValue();
            
            /* Test whether the connection to the node is OK */
			if (!statusTest(nodeName)) {
				continue;
			}

			ObjectInputStream nodeInput = node.getOIS();
			ObjectOutputStream nodeOutput = node.getOOS();

			try {
            	/* Terminate the slave */
				nodeOutput.writeObject("terminate");
				String response = (String) nodeInput.readObject();
				if (response.equals("OK")) {
					System.out.println(nodeName + " exit successfully!");
					node.deleteSlave();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
        /* Stop daemon thread */
		daemon.stop();
		try {
			daemonThread.join();
		} catch (InterruptedException e) {
			// ignore
		}
	}

	/* statusTest - Test the connection between master and slave */
	public boolean statusTest(String nodeName) {

		SlaveState node = nameSlaveMap.get(nodeName);
		ObjectInputStream nodeInput = node.getOIS();
		ObjectOutputStream nodeOutput = node.getOOS();
		boolean accessible = false;

		try {
        	/* "ping" the slave node */
			nodeOutput.writeObject("ping");
            /* Get alive response */
			String response = (String) nodeInput.readObject();
			if (response.equals("alive")) {
				accessible = true;
			}
		} catch (IOException e) {
        	/* Cannot get slave response */
			System.out.println(nodeName + " is not reachable!");
			System.out.println("remove " + nodeName + " from node list!");
			accessible = false;
		} catch (ClassNotFoundException e) {
			// ignore
		}

        /* Slave node is not accessible, remove the slave from the slave list */
		if (!accessible) {
        	/* Remove all process that is running on the node */
			for (Map.Entry<Integer, ProcessState> entry :
					processMap.entrySet()) {
				int pid = entry.getKey();
				ProcessState ps = entry.getValue();
				if (nodeName.equals(ps.getNodeName())) {
					this.processMap.remove(pid);
				}
			}
			nameSlaveMap.remove(nodeName);
		}

		return accessible;
	}

	/* RegisterDaemon - daemon that accept slave node registration */
	class RegisterDaemon implements Runnable {
		private ServerSocket server;
		private boolean running;

		public RegisterDaemon() {
			running = true;
		}

		@Override
		public void run() {
			try {
				server = new ServerSocket(LISTENPORT);	/* Listening on 15640 */
				while (running) {
	            	/* A slave connects */
					Socket socket = server.accept();

					ObjectOutputStream output =
							new ObjectOutputStream(socket.getOutputStream());
					ObjectInputStream input =
							new ObjectInputStream(socket.getInputStream());

                    /* A register line should be "register name" */
					String registerLine = (String) input.readObject();
					String[] registerCmd = registerLine.split(" ");

					if (registerCmd[0].equals("register")) {
                    	/* A slave want to register to master */
						String nodeName = registerCmd[1];

						if (!nameSlaveMap.containsKey(nodeName)) {
                        	/* The slave node has not registered */
							SlaveState node = new SlaveState(nodeName,
									socket,
									input,
									output);
                            /* Record this node */
							nameSlaveMap.put(nodeName, node);
							System.out.println(nodeName + " is registered!");
							System.out.print(prompt);
                            /* Send master response back to slave node */
							output.writeObject("OK");
						} else {
                        	/* Send master response back to slave node */
							output.writeObject("exist");
						}

					}
				}
			} catch (IOException e) {
            	/* Cannot get registration line */
				System.out.println("connection to slave is terminated");
			} catch (ClassNotFoundException e) {
				// ignore
			}
		}

		/* Stop the running thread */
		public void stop() {
			running = false;
			try {
				server.close();
			} catch (IOException e) {
				System.out.println("IOException: daemon ServerSocket close() wrong!");
			}
		}
	} /* End RegisterDaemon */
}
