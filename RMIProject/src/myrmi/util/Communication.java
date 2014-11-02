package myrmi.util;

/**
 * Communication.java
 * 
 * @author Tong Wei, Guoli Ma {twei1, guolim}@andrew.cmu.edu
 * 
 * This class handles all communication tasks: sending a message to a host and
 * receiving a message from a host.
 */

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class Communication {
	/**
	 * sendMessage - Send a message to a specified host
	 * @param message - The message.
	 * @param socket - The socket that connects to the host.
	 * @return Whether message is sent.
	 */
	public static boolean sendMessage(RMIMessage message, Socket socket) {
        boolean success = false;
		try {
			/* Send message */
	        ObjectOutputStream output = 
	                new ObjectOutputStream(socket.getOutputStream());
	        output.writeObject(message);
            success = true;
        } catch (IOException e) {
            System.out.println("Communication: Connection error");
            e.printStackTrace();
        }
        return success;
	}
	
	/**
	 * receiveMessage - Receive a message from a host.
	 * @param socket - The socket that connects to the host.
	 * @return The received message.
	 */
	public static RMIMessage receiveMessage(Socket socket) {
		RMIMessage message = null;
		try {
	        ObjectInputStream input = 
	                new ObjectInputStream(socket.getInputStream());
	        message = (RMIMessage)input.readObject();
        } catch (IOException e) {
            System.out.println("Communication: Connection error");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.out.println("Communication: Received wrong object");
            e.printStackTrace();
        }
		return message;
    }
}
