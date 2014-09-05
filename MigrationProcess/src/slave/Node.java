package slave;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Scanner;
import tcpThread.*;

/**
 * Created by Wei on 9/4/14.
 */
public class Node {
	private String nodeName;
	private int masterPort;

	public void start() {
		System.out.println("Node " + this.nodeName + "started.");
	}

	// Connect to master and communicate with master in a new thread.
	public void connectToMaster() {
		TcpClientThread tcpClientThread = new TcpClientThread("localhost", this.masterPort);

		new Thread(tcpClientThread).start();
	}

	public void parseCmd(String cmdLine) {
		String[] args = cmdLine.split(" ");
	}

	/*
	 * Getters and setters.
	 */
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public void setMasterPort(int masterPort) {
		this.masterPort = masterPort;
	}

	public int getMasterPort() {
		return masterPort;
	}

	public String getNodeName() {

		return nodeName;
	}

	/*
	* Main function.
	*/
	public static void main(String[] args) {
		// Init node and start it.
		Node node = new Node();
		node.setNodeName(args[1]);
		node.setMasterPort(Integer.parseInt(args[2]));
		node.start();

		node.connectToMaster();


	}
}



class Tcp
