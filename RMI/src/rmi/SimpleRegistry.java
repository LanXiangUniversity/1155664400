package rmi;

import remote.RemoteObjectRef;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by Wei on 10/3/14.
 */

// Create a local reference to the registry.
public class SimpleRegistry {
	// Registry holds its port and host, connects to it each time.
	private String host;
	private int port;

	public SimpleRegistry(String host, int port) {
		this.host = host;
		this.port = port;
	}

	// Return the ROR (if found) or null (if else)
	public RemoteObjectRef lookup(String serviceName) throws IOException {
		Socket sock = new Socket(host, port);

		System.out.println("Socket made.");

		// Get TCP streams and wrap them.
		BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
		PrintWriter out = new PrintWriter(sock.getOutputStream());

		System.out.println("Streams made.");

		// Locate request with service name.
		out.println("lookup");
		out.println(serviceName);

		System.out.println("Command and service name sent.");

		// Branch according to the answer.
		String res = in.readLine();
		RemoteObjectRef ror;

		if ("found".equals(res)) {
			System.out.println("It is found.");

			// Receive ROR data.
			String rorIPAddr = in.readLine();
			System.out.println("rorIPAddr: " + rorIPAddr);

			int rorPortNum = Integer.parseInt(in.readLine());
			System.out.println("rorPortNum: " + rorPortNum);

			int rorObjKey = Integer.parseInt(in.readLine());
			System.out.println("rorObjKey: " + rorObjKey);

			String rorInterfaceName = in.readLine();
			System.out.println("rorInterfaceName: " + rorInterfaceName);

			// Make ror.
			ror = new RemoteObjectRef(rorIPAddr, rorPortNum, rorObjKey, rorInterfaceName);
		} else {
			System.out.println("It is not found.");
			ror = null;
		}

		sock.close();

		return ror;
	}

	// Rebind a ROR (can be null).
	public void rebind(String serviceName, RemoteObjectRef ror) throws IOException {
		// Open socket, same as before.
		Socket sock = new Socket(this.host, this.port);

		// Get TCP streams and wrap them.
		BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
		PrintWriter out = new PrintWriter(sock.getOutputStream());

		// It is a rebind request, with a service name and ROR.
		out.println("rebind");
		out.println(serviceName);
		out.println(ror.getIPAddress());
		out.println(ror.getPort());
		out.println(ror.getObjKey());
		out.println(ror.getRemoteInterfaceName());

		// It also get an ack.
		String sck = in.readLine();

		sock.close();
	}
}
