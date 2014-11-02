/**
 * RMIRegistry.java
 *
 * @author Tong Wei, Guoli Ma {twei1, guolim}@andrew.cmu.edu
 *
 * This is the simulation for Java's native RMIRegistry. It can generate a
 * Registry class and send it to its client and accept Remote object binding,
 * looking up, unbinding and rebinding.
 */

import myrmi.Remote;
import myrmi.registry.Registry;
import myrmi.util.Communication;
import myrmi.util.RMIMessage;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

public class RMIRegistry {
	private ServerSocket serverSock;
	/* Listening port, default = Registry.REGISTRY_PORT = 15640 */
	private int port;
	private HashMap<String, Remote> stubMap = new HashMap<String, Remote>();

	/**
	 * Constructor.
	 *
	 * @param port - Registry listening port.
	 */
	public RMIRegistry(int port) {
		this.port = port;
		try {
			serverSock = new ServerSocket(port);
		} catch (IOException e) {
			System.out.println("RMIRegistry starting error");
			e.printStackTrace();
			System.exit(-1);
		}

		stubMap = new HashMap<String, Remote>();
	}

	private static final void Usage() {
		System.out.println("Usage:");
		System.out.println("RMIRegistry [<port number>]");
	}

	public static void main(String[] args) {
		int port = Registry.REGISTRY_PORT;

        /* Parse user input port */
		if (args.length == 1) {
			try {
				port = Integer.parseInt(args[0]);
			} catch (NumberFormatException e) {
				Usage();
				System.out.println("<port number> error");
				System.exit(-1);
			}
			if (port < 1024 || port > 65535) {
				Usage();
				System.out.println("<port number> range: 1024 ~ 65535");
				System.exit(-1);
			}
		} else if (args.length > 1) {
			Usage();
			System.exit(-1);
		}

		RMIRegistry rmiRegistry = new RMIRegistry(port);

		Thread listenerThread = new Thread(rmiRegistry.new Listener());
		listenerThread.start();

		System.out.println("RMIRegistry starts at port: " + port);
	}

	/**
	 * Listener - Listen for registry event like lookup, bind and update stub
	 * map according to received message.
	 */
	private class Listener implements Runnable {
		/**
		 * sendRegistry - Send the Registry class to client.
		 */
		private void sendRegistry(Socket socket) {
			Registry registry = null;
			String host = "";
			String responseName = "OK";

            /* Get host address of this RMIRegistry. */
			try {
				host = InetAddress.getLocalHost().getHostAddress();
				registry = new Registry(host, port);
			} catch (UnknownHostException e) {
				responseName = "RMIRegistry cannot get its own host name";
				System.out.println(responseName);
				System.exit(-1);
			}

            /* Send back the registry */
			RMIMessage message =
					new RMIMessage()
							.withMessageType(RMIMessage.MessageType.RESPONSE)
							.withCarrier(registry)
							.withName(responseName);

			Communication.sendMessage(message, socket);
		}

		/**
		 * lookup - Look up the service name in the stub map and send back
		 * the stub founded.
		 *
		 * @param socket - Connection to client
		 * @param name   - Service name to be searched
		 */
		public void lookup(Socket socket, String name) {
			Remote stub = null;
			String responseName = "";

            /* Look up the service */
			if (stubMap.containsKey(name)) {
				stub = stubMap.get(name);
				responseName = "OK";
				System.out.println("Lookup for " + name + " OK");
			} else {
				responseName = "RMIRegistry cannot find service: " + name;
				System.out.println("Lookup error: " + responseName);
			}

            /* Send back a response with the stub */
			RMIMessage message =
					new RMIMessage()
							.withMessageType(RMIMessage.MessageType.RESPONSE)
							.withName(responseName)
							.withCarrier(stub);

			Communication.sendMessage(message, socket);
		}

		/**
		 * bind - Bind a service name with a stub and stored them in the map.
		 *
		 * @param socket - Connection to client
		 * @param name   - Service name
		 * @param stub   - The stub to be binded
		 */
		public void bind(Socket socket, String name, Remote stub) {
			String responseName = "";

            /* Make sure the name is not used yet */
			if (!stubMap.containsKey(name)) {
				stubMap.put(name, stub);
				responseName = "OK";
				System.out.println("Bind " + name + " OK");
			} else {
				responseName = "Service " + name + " has been binded";
				System.out.println("Bind error: " + responseName);
			}

            /* Send bind result response */
			RMIMessage message =
					new RMIMessage()
							.withMessageType(RMIMessage.MessageType.RESPONSE)
							.withName(responseName);
			Communication.sendMessage(message, socket);
		}

		/**
		 * unbind - Unbind a service name.
		 *
		 * @param socket - Connection to client
		 * @param name   - Service name
		 */
		public void unbind(Socket socket, String name) {
			String responseName = "";
			if (stubMap.containsKey(name)) {
				stubMap.remove(name);
				responseName = "OK";
				System.out.println("Unbind " + name + " OK");
			} else {
				responseName = "Service " + name + " does not exit";
				System.out.println("Unbind error: " + responseName);
			}

			RMIMessage message =
					new RMIMessage()
							.withMessageType(RMIMessage.MessageType.RESPONSE)
							.withName(responseName);
			Communication.sendMessage(message, socket);
		}

		/**
		 * rebind - Rebind a service name with a new stub.
		 *
		 * @param socket - Connection to client
		 * @param name   - New service name
		 * @param stub   - The stub to be rebinded
		 */
		public void rebind(Socket socket, String name, Remote stub) {
			if (stubMap.containsKey(name)) {
				stubMap.remove(name);
			}
			stubMap.put(name, stub);
			String responseName = "OK";

			RMIMessage message =
					new RMIMessage()
							.withMessageType(RMIMessage.MessageType.RESPONSE)
							.withName(responseName);
			Communication.sendMessage(message, socket);
		}

		/**
		 * list - List all service name in the map.
		 *
		 * @param socket - Connection to client
		 */
		public void list(Socket socket) {
			String carrier = "";

			for (String name : stubMap.keySet()) {
				carrier = carrier + name + " ";
			}
			carrier = carrier.trim();

			RMIMessage message =
					new RMIMessage()
							.withMessageType(RMIMessage.MessageType.RESPONSE)
							.withName("OK")
							.withCarrier(carrier);
			Communication.sendMessage(message, socket);
		}

		/**
		 * run - The main thread of the registry.
		 * Receive a message from the client and do something according to the
		 * message.
		 */
		public void run() {
			while (true) {
				try {
					Socket socket = serverSock.accept();

					RMIMessage message = Communication.receiveMessage(socket);
					String name = message.getName();
					Object obj = message.getCarrier();

					switch (message.getMessageType()) {
						case GET_REGISTRY:
							System.out.println("Received GET_REGISTRY message");
							sendRegistry(socket);
							break;
						case LOOK_UP:
							lookup(socket, name);
							break;
						case BIND:
							bind(socket, name, (Remote) obj);
							break;
						case UNBIND:
							unbind(socket, name);
							break;
						case REBIND:
							rebind(socket, name, (Remote) obj);
							break;
						case LIST:
							list(socket);
							break;
						default:
							String error = "RMIRegistry received wrong message";
							System.out.println(error);
							break;
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
