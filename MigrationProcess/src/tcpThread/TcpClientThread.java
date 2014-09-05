package tcpThread;

import java.io.DataInputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Created by Wei on 9/5/14.
 */
class TcpClientThread implements Runnable {
	private int remotePort;
	private String remoteHost;
	private Socket reqSock;

	TcpClientThread() {}

	TcpClientThread(String remoteHost, int remotePort) {
		this.remoteHost = remoteHost;
		this.remotePort = remotePort;

		try {
			this.reqSock = new Socket(InetAddress.getByName(remoteHost), remotePort);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	TcpClientThread(Socket reqSock) {
		this.reqSock = reqSock;
	}

	/*
	 * Run a socket connection.
	 */
	@Override
	public void run() {
		// Connect to master
		System.out.println("Connected to master.");

		try {
			// Get IO streams
			DataInputStream in = new DataInputStream(reqSock.getInputStream());
			PrintStream out = new PrintStream(reqSock.getOutputStream());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}