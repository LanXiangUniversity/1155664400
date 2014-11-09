/**
 * Slave.java
 * @author Tong Wei (twei1), Guoli Ma (guolim)
 *
 * Description: The slave node class. When starting a slave, it will first
 * 				register to the master server. Then the slave will continuously
 * 				receive commands from master and do the specified jobs.
 */

package slave;

import processes.MigratableProcess;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Slave {
	private final int MASTERPORT = 15640;
	private String nodeName;
	private String masterAddr;
	private Socket socket;
	private ObjectInputStream input;
	private ObjectOutputStream output;
	/* key = process id, value = a migratable process */
	private ConcurrentHashMap<Integer, MigratableProcess> pidProcessMap;
	/* key = process id, value = a thread that is running migratable process */
	private ConcurrentHashMap<Integer, Thread> pidThreadMap;
	/* A list of all terminated process */
	private List<Integer> terminatedPID;
	/* A listener that test whether processes are finished */
	private ProcessListener processListener;
	private Thread threadReaper;

	/* Constructor */
	public Slave(String name, String addr) {
		this.nodeName = name;
		this.masterAddr = addr;
		this.pidProcessMap = new ConcurrentHashMap<Integer, MigratableProcess>();
		this.pidThreadMap = new ConcurrentHashMap<Integer, Thread>();
		this.terminatedPID = Collections.synchronizedList(
				new ArrayList<Integer>());
		this.processListener = new ProcessListener();
		this.threadReaper = new Thread(processListener);
		this.threadReaper.start();
		System.out.println("Node " + nodeName + " starts.");
	}

	/* main - Main routine of slave node */
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("USAGE:");
			System.out.println("Slave <node name> <master ip>");
			return;
		}
		Slave slave = new Slave(args[0], args[1]);

		if (!slave.register()) {
			System.out.println(args[0] + " terminated!");
			System.exit(1);
		}

		while (true) {
			String[] commands = slave.getCommand();

			if (commands == null) {
				continue;

			} else if (commands[0].equals("ping")) {
				slave.pingResponse();

			} else if (commands[0].equals("launch")) {
                /* launch processName PID args */
				System.out.print("launch received...");
				slave.launch(Integer.parseInt(commands[2]), // PID
						commands[1], // processName
						Arrays.copyOfRange(commands, 3, commands.length));

			} else if (commands[0].equals("migrate")) {
                /* migrate pid */
				System.out.print("migrate received...");
				slave.migrate(Integer.parseInt(commands[1]));

			} else if (commands[0].equals("resume")) {
                /* resume pid */
				System.out.print("resume received...");
				slave.resume(Integer.parseInt(commands[1]));

			} else if (commands[0].equals("remove")) {
                /* remove pid */
				System.out.print("remove received...");
				slave.remove(Integer.parseInt(commands[1]));

			} else if (commands[0].equals("ps")) {
				System.out.println("ps received!");
				slave.ps();

			} else if (commands[0].equals("terminate")) {
				break;
			}
		}
		slave.exit();
		System.exit(0);
	}

	/* register - Slave send register request to master. */
	public boolean register() {
		boolean done = false;
		if (socket == null) {
			System.out.print("Registering...");
			try {
				this.socket = new Socket(this.masterAddr, this.MASTERPORT);
				this.input = new ObjectInputStream(socket.getInputStream());
				this.output = new ObjectOutputStream(socket.getOutputStream());
                /* Send register command */
				this.output.writeObject("register " + this.nodeName);
                /* Receive response */
				String response = (String) input.readObject();

				if (response.equals("OK")) {
					System.out.println("done!");
					done = true;
				} else if (response.equals("exist")) {
					System.out.println("\nError: slave name " +
							this.nodeName +
							" already registered!");
				}
			} catch (UnknownHostException e) {
				System.out.println("Failed!");
				System.out.println("Unknown host!");
			} catch (IOException e) {
				System.out.println("Failed!");
				System.out.println("Cannot register to master!");
			} catch (ClassNotFoundException e) {
				// ignore
			}
		}
		return done;
	}

	/* getCommand - Receive command from master */
	public String[] getCommand() {
		String commands = null;
		try {
        	/* Receive command */
			commands = (String) input.readObject();
		} catch (IOException e) {
			System.out.println("Lose connection to master!");
			return (new String[]{"terminate"});
		} catch (ClassNotFoundException e) {
			// ignore
		}
		return commands.split(" ");
	}

	/* launch - Launch a new migrate processes */
	public void launch(int pid, String processName, String[] args) {
		boolean done = false;
		MigratableProcess process = null;
		String response = "OK";

		try {
        	/* Construct the process using java reflection */
			Class<?> processClass = Class.forName(processName);
			Constructor<?> ctor = processClass.getConstructor(String[].class);
            /* Process argument */
			Object[] argument = {args};
            /* Create process */
			process = (MigratableProcess) ctor.newInstance(argument);
			done = true;
		} catch (ClassNotFoundException e) {
			response = "no such processName: " + processName;
			System.out.println("error: Slave received wrong processName.");
		} catch (Exception e) {
			response = "slave cannot instantiate " + processName;
			System.out.println("error: Slave cannot instantiate " + processName);
		}

		if (done) {
        	/* Launching succeeded */
			System.out.println("done!");
			System.out.println("launching " + processName);
        	/* Add process to process map and start running */
			this.pidProcessMap.put(pid, process);
			Thread thread = new Thread(process);
			this.pidThreadMap.put(pid, thread);
			thread.start();
		}

		try {
        	/* Send response to server */
			output.writeObject(response);
		} catch (IOException e) {
			System.out.println("IOException: write launch response wrong!");
		}
	}

	/* migrate - Stop a process and send it to master */
	public void migrate(int pid) {
		try {
        	/* Test whether pid is running */
			if (terminatedPID.contains(new Integer(pid))) {
				this.output.writeObject(null);
				this.output.writeObject("reaped");
				return;
			}
        	/* Stop target process and send it to master */
			MigratableProcess targetProcess = this.pidProcessMap.get(pid);
			targetProcess.suspend();
			this.output.writeObject(targetProcess);
			this.output.writeObject("OK");
			System.out.println("done!");
		} catch (IOException e) {
			System.out.println("error migrating process to master");
		}
		this.pidProcessMap.remove(pid);
		this.pidThreadMap.remove(pid);
		if (terminatedPID.contains(new Integer(pid))) {
			terminatedPID.remove(new Integer(pid));
		}
	}

	/* resume - Receive a migrated process from master and resume its running */
	public void resume(int pid) {
		MigratableProcess newProcess = null;
		try {
        	/* Receive a migrated process from master */
			newProcess = (MigratableProcess) input.readObject();
			this.pidProcessMap.put(pid, newProcess);

            /* Resume its running */
			Thread newThread = new Thread(newProcess);
			this.pidThreadMap.put(pid, newThread);
			newThread.start();
            
            /* Send response */
			this.output.writeObject("OK");
			System.out.println("done!");
		} catch (IOException e) {
			System.out.println("IOException: error receiving process from master");
		} catch (ClassNotFoundException e) {
			// ignore
		}
	}

	/* remove - Stop a running process and remove it from process list */
	public void remove(int pid) {
		MigratableProcess process = pidProcessMap.get(pid);
		String processName = process.getClass().getName();
		Thread thread = pidThreadMap.get(pid);
        
        /* Stop the process */
		process.suspend();
		try {
			thread.join();
		} catch (InterruptedException e) {
			// ignore
		}
        
        /* Remove the process from the process list */
		pidProcessMap.remove(pid);
		pidThreadMap.remove(pid);
        
        /* Send response */
		System.out.println("done!");
		System.out.println(processName + " removed successfully!");
		try {
			output.writeObject("OK");
		} catch (IOException e) {
			System.out.println("error writing remove response");
		}
	}

	/* ps - Send a terminated processes list to master */
	public void ps() {
		try {
			String terminatedProcesses = new String();
			for (Integer pid : this.terminatedPID) {
				terminatedProcesses += (pid.toString() + " ");
			}
			output.writeObject(terminatedProcesses.trim());
		} catch (Exception e) {
			e.printStackTrace();
		}
		terminatedPID.clear();
	}

	/* pingResponse - Send alive response for *ping* command */
	public void pingResponse() {
		try {
			output.writeObject("alive");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/* exit - Terminate the slave node, stop all running processes */
	public void exit() {
		for (MigratableProcess process : pidProcessMap.values()) {
			process.suspend();
		}

		try {
			processListener.stop();
			threadReaper.join();
			output.writeObject("OK");
			socket.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(this.nodeName + " terminated!");
	}

	/* ProcessListener - Thread that test whether processes are finished */
	private class ProcessListener implements Runnable {
		private boolean running;

		public ProcessListener() {
			running = true;
		}

		@Override
		public void run() {
			while (running) {
				for (Map.Entry<Integer, Thread> entry :
						pidThreadMap.entrySet()) {
					Integer pid = entry.getKey();
					Thread thread = entry.getValue();
	                /* For each process, test whether it is running */
					if (!thread.isAlive()) {
						try {
							thread.join();
						} catch (Exception e) {
							e.printStackTrace();
						}
						pidProcessMap.remove(pid);
						pidThreadMap.remove(pid);
						terminatedPID.add(new Integer(pid));
						System.out.println("Process " + pid + " is reaped!");
					}
				}
			}
		}

		public void stop() {
			running = false;
		}
	}
}
