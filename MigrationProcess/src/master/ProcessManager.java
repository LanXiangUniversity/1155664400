package master;

import java.io.DataInputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

/**
 * Created by Tong Wei on 8/28/14.
 */
public class ProcessManager {
	// Command line parser (robust!!!)
	// Launching a process
	// Joining terminated threads
	// Lastly, the distributed part
	private String pomptStr;

	public ProcessManager() {
		pomptStr = "lab1Cmd >> ";
	}

	public void parseCmd(String args) {

	}

	public static void main(String[] args) {
		String cmdLine;
		ProcessManager pm = new ProcessManager();
		Scanner in = new Scanner(System.in);

		System.out.println("Master started.");

		new Thread(new ListenThread()).start();
		System.out.println("Listen thread started.");

		while (true) {
			System.out.print(pm.pomptStr);

			pm.parseCmd(cmdLine = in.nextLine());

		}
	}
}

class ListenThread implements Runnable {
	private static final int LISTEN_PORT = 8011;
	private static final int THREAD_ID = 17;

	@Override
	public void run() {
		try {
			ServerSocket srvSock = new ServerSocket(38887);
				System.out.println("\n\t\tlisten for new node");
			Socket reqSock = srvSock.accept();
				System.out.println("\t\tnew node connected");

			DataInputStream in = new DataInputStream(reqSock.getInputStream());
			PrintStream out = new PrintStream(reqSock.getOutputStream());


			System.out.print(this.THREAD_ID + ": " + in.readLine());

			in.close();
			out.close();
			srvSock.close();
			reqSock.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
