package slave;

import processes.MigratableProcess;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Wei on 9/4/14.
 */
public class Node1 implements Runnable{

	private String nodeName;
	private int masterPort;
	private Socket sock;
	private ObjectOutputStream oos;
	private ObjectInputStream ois;
	private ProcessListener processListener;
	private final ConcurrentHashMap<Integer, Thread> threadList;
	private final ConcurrentHashMap<Integer, MigratableProcess> processList;
	private List<Integer> terminatedList;

	public Node1(String nodeName, int masterPort) {
		this.nodeName = nodeName;
		this.masterPort = masterPort;

		this.threadList = new ConcurrentHashMap<Integer, Thread>();
		this.processList = new ConcurrentHashMap<Integer, MigratableProcess>();
		this.terminatedList = Collections.synchronizedList(new ArrayList<Integer>());
		System.out.println("Node " + this.nodeName + " starts.");
	}

	// Wait for command and process (Object) from master.
	@Override
	public void run() {
		while(true) {
			try {
				// Read cmd from master
				String cmdLine = (String) ois.readObject();
				parseCmd(cmdLine);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	//
	public void parseCmd(String cmdLine) {
		try {
			String[] args = cmdLine.split("#");
			// Launch a new process.
			if ("launch".equals(args[0])) {
				//Object obj = ois.readObject();
				//Class<?> c = obj.getClass();
				int pid = Integer.parseInt(args[1]);

				String processStr = (String) ois.readObject();

				String[] vars = processStr.split(" ");
				String argStr = "";
				for (int i = 1; i < vars.length; i++) {
					argStr += (" " + vars[i]);
				}

				String[] ss = argStr.substring(1).split(" ");

				Class<?> c = Class.forName("processes." + vars[0]);
				Constructor[] ctrs = c.getConstructors();
				MigratableProcess mp = null;

				for (Constructor ctr : ctrs) {
					if (ctr.getParameterTypes().length == 1) {
						mp = (MigratableProcess) ctr.newInstance((Object)ss);
						break;
					}
				}

				Thread thread = new Thread(mp);
				this.threadList.put(pid, thread);
				this.processList.put(pid, mp);
				thread.start();
			} else if ("migrate".equals(args[0])) {
				int pid = Integer.parseInt(args[1]);

				MigratableProcess mp = this.processList.get(pid);

				mp.suspend();
				this.threadList.remove(pid);
				this.processList.remove(pid);
				oos.writeObject(mp);
				System.out.println("migrate process " + pid);
			} else if ("kill".equals(args[0])) {
				int pid = Integer.parseInt(args[1]);

				MigratableProcess mp = this.processList.get(pid);
				mp.suspend();
				this.threadList.remove(pid);
				this.processList.remove(pid);
				System.out.println("Process " + pid + " was kill.");
			} else if ("launch_migrated".equals(args[0])) {
				int pid = Integer.parseInt(args[1]);

				MigratableProcess mp = (MigratableProcess) ois.readObject();

				Thread thread = new Thread(mp);
				this.threadList.put(pid, thread);
				this.processList.put(pid, mp);
				thread.start();
			} else if ("ps".equals(args[0])) {
				this.oos.writeObject(this.terminatedList);
			}
  		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Connect to master and communicate with master in a new thread.
	public void connectToMaster() {
		try {
			// Reap terminated threads.
			this.processListener = new ProcessListener();
			new Thread(this.processListener).start();

			// Connect to master.
			this.sock = new Socket("localhost", this.masterPort);
			this.ois = new ObjectInputStream(new DataInputStream(sock.getInputStream()));
			this.oos = new ObjectOutputStream(new DataOutputStream(sock.getOutputStream()));

			// Send node info to master.
			this.oos.writeObject(new String(this.nodeName));
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Wait for msg from master.
	}

	// Reap terminated threads, move the pid of terminated thread into terminatedList
	class ProcessListener implements Runnable {
		@Override
		public void run() {
			while (true) {
				for (int key : threadList.keySet()) {
					if (!threadList.get(key).isAlive()) {

						System.out.println("process " + key + " is reaped" );

						try {
							threadList.get(key).join();
						} catch (Exception e) {
							e.printStackTrace();
						}

						threadList.remove(key);
						processList.remove(key);

						terminatedList.add(key);
					}
				}

				try {
					Thread.sleep(100);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	/*
	* Main function.
	*/
	public static void main(String[] args) {
		// Init node and start it.
		Node1 node = new Node1("node2", 38887);
		node.connectToMaster();

		new Thread(node).start();

		while (true) {}
	}
}