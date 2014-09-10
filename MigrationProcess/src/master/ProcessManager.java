package master;

import processes.CountProcess;
import processes.MigratableProcess;
import processes.ProcessState;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Tong Wei on 8/28/14.
 */
public class ProcessManager {
	// Command line parser (robust!!!)
	// Launching a process
	// Joining terminated threads
	// Lastly, the distributed part
	private String pomptStr;
	private ListenThread listenThread;
	private TerminalThread terminalThread;
	private int listen_port;
	private ServerSocket srvSock;
	private boolean isRun = true;
	private int pid = 0;
	private ConcurrentHashMap<Integer, ProcessState> processList;
	private ConcurrentHashMap<String, Socket> sockList;
	// ObjectIOStream is locked.
	private ConcurrentHashMap<String, ObjectOutputStream> oosList;
	private ConcurrentHashMap<String, ObjectInputStream> oisList;

	public ProcessManager() {
		this.processList = new ConcurrentHashMap<Integer, ProcessState>();
		this.sockList = new ConcurrentHashMap<String, Socket>();
		this.oosList = new ConcurrentHashMap<String, ObjectOutputStream>();
		this.oisList = new ConcurrentHashMap<String, ObjectInputStream>();

		this.listen_port = 38887;
		this.pomptStr = "mcdsh >> ";
		this.listenThread = new ListenThread();
		this.terminalThread = new TerminalThread();
	}

	// Start listening for nodes.
	public void startListen() {
		new Thread(this.listenThread).start();
		System.out.println("Listening started, port = " + this.listen_port);
	}

	public void startTerminal() {
		Scanner in = new Scanner(System.in);
		String cmdLine;

		// Process cmd lines.
		while (true) {
			System.out.print(this.pomptStr);
			cmdLine = in.nextLine();

			this.parseCmd(cmdLine);
		}
	}

	public void parseCmd(String cmdLine) {
		String[] args = (cmdLine).split(" ");

		String usage = "usage: count <number>";

		// count <number> <nodeName>
		if ("count".equals(args[0])) {
			try {
				if (args.length == 3) {
					int cnt = Integer.parseInt(args[1]);

					// Add new process to processList
					this.processList.put(this.pid, new ProcessState(this.pid, args[0], args[2]));

					String processStr = "CountProcess" + " " +args[1];

					// Launch a new process
					launch(processStr, args[2]);

					this.pid++;
				} else {
					System.out.println("\nIllegal Arguments");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// grep <query> <infile> <outfile> <nodeName>
		else if ("grep".equals(args[0])) {
			if (args.length == 5) {
				// Add new process to processList
				this.processList.put(this.pid, new ProcessState(this.pid, args[0], args[4]));

				String processStr = "GrepProcess " + args[1] + " " + args[2] + " " + args[3];

				// Launch a new process
				launch(processStr, args[4]);

				this.pid++;
			} else {
				System.out.println("\nIllegal Arguments");
			}
		}
		// ps
		else if ("ps".equals(args[0])) {
			if (args.length == 1) {
				this.ps();
			} else {
				System.out.println("\nIllegal Arguments");
			}
		}
		// migrate <pid> <nid> <nid>
		else if ("migrate".equals(args[0])) {
			if (args.length == 3) {
				this.migrate(Integer.parseInt(args[1]), args[2]);
			} else {
				System.out.println("\nIllegal Arguments");
			}
		}
		// stop <pid>
		else if ("kill".equals(args[0])) {
			if (args.length == 2) {
				kill(Integer.parseInt(args[1]));
			} else {
				System.out.println("\nIllegal Arguments");
			}
		}
	}

	public void launch(String processStr, String nodeName) {
		System.out.println("\nlaunch " + this.pid);
		try {
			ObjectOutputStream oos = this.oosList.get(nodeName);
			oos.writeObject(new String("launch#" + this.pid));
			oos.writeObject(new String(processStr));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void migrate(int pid, String dstNode) {
		ProcessState ps = this.processList.get(pid);
		String srcNode = ps.getNodeName();
		//Socket dstSock = this.sockList.get(dstNode);

		try {
			// Get process from srcNode.
			ObjectOutputStream oos = this.oosList.get(srcNode);

			ObjectInputStream ois = this.oisList.get(srcNode);

			// Send cmd to nodes.
			oos.writeObject(new String("migrate#" + pid));
			oos = this.oosList.get(dstNode);
			Object mp = ois.readObject();
			oos.writeObject(new String("launch_migrated#" + pid));
			oos.writeObject(mp);
			this.processList.get(pid).setNodeName(dstNode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void ps() {
		// Get reaped processes from each node.
		for (String nodeName : this.sockList.keySet()) {
			ObjectOutputStream oos = this.oosList.get(nodeName);
			ObjectInputStream ois = this.oisList.get(nodeName);

			try {
				oos.writeObject(new String("ps"));

				List<Integer> list = (List<Integer>) ois.readObject();

				for (int key : list) {
					this.processList.remove(key);
					//System.out.println(key + " is terminated");
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		System.out.println("\nPID\tNID\t\tCMD");

		for (ProcessState ps : processList.values()) {
			System.out.println(ps.getPid()+"\t"+ps.getNodeName()+
					"\t" + ps.getProcessName());
		}
	}

	public void kill(int pid) {
		ProcessState ps = this.processList.get(pid);
		String nodeName = ps.getNodeName();
		ObjectOutputStream oos = this.oosList.get(nodeName);

		this.processList.remove(pid);

		try {
			oos.writeObject(new String("kill#" + pid));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	class ListenThread implements Runnable {
		public ListenThread () {
			try {
				srvSock = new ServerSocket(listen_port);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void run() {
			try {
				while (true) {
					Socket sock = srvSock.accept();

					ObjectOutputStream oos = new ObjectOutputStream(
							new DataOutputStream(sock.getOutputStream()));
					ObjectInputStream ois = new ObjectInputStream(
							new DataInputStream(sock.getInputStream()));

					String nodeName = (String) ois.readObject();

					// Add connection into hashmap.
					sockList.put(nodeName, sock);
					oisList.put(nodeName, ois);
					oosList.put(nodeName, oos);
					System.out.print("\n[MSG] Node " + nodeName + " connected.\n" + pomptStr);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	class TerminalThread implements Runnable {
		@Override
		public void run() {
			Scanner in = new Scanner(System.in);
			String cmdLine;

			// Process cmd lines.
			while (true) {
				System.out.print(pomptStr);
				cmdLine = in.nextLine();

				parseCmd(cmdLine);
			}
		}
	}

	public static void main(String[] args) {
		ProcessManager pm = new ProcessManager();

		String cmdLine;

		System.out.println("Master started.");

		pm.startListen();
		System.out.println("Listen thread started.");
		pm.startTerminal();

		while (pm.isRun) {}
	}
}
