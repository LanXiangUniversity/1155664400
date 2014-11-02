package myrmi.server;

/**
 * RemoteRef.java
 * 
 * @author Tong Wei, Guoli Ma {twei1, guolim}@andrew.cmu.edu
 * 
 * A simulation of java.rmi.server.RemoteRef.
 * It is a remote object reference. This class helps to record what the object 
 * is (A object ID) and to locate where the remote object locate (Its proxy 
 * dispatcher host name and port. 
 */

import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;

import myrmi.util.Communication;
import myrmi.util.RMIMessage;

public class RemoteRef implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6971167066485948815L;
	
	private String objID;
	private String host;
	private int port;
	
	public RemoteRef(String objID, String host, int port) {
		this.objID = objID;
        this.host = host;
        this.port = port;
	}
	
	public String getObjID() {
		return this.objID;
	}
	
	public String getHost() {
		return this.host;
	}
	
	public int getPort() {
		return this.port;
	}
	
	/**
	 * invoke - All remote object stub will call this method to marshell a 
	 * 			remote call message and send the message to its proxy.
	 * @param methodName - The method name of remote method invocation
	 * @param args - The arguments of the remote method.
	 * @return The return value of the remote method.
	 */
	public Object invoke(String methodName, Object[] args) {
		Socket socket = null;
		Object returnValue = null;
		try {
			socket = new Socket(host, port);
			System.out.println("Try to invoke " + methodName + 
							   " on " + host + ":" + port);
		
			RMIMessage message = new RMIMessage()
								 .withMessageType(RMIMessage.MessageType.REMOTE_CALL)
								 .withName(methodName)
								 .withCarrier(objID)
								 .withArgs(args);
			
			if (Communication.sendMessage(message, socket) == true) {
				message = Communication.receiveMessage(socket);
				if (RMIMessage.checkResponse(message)) {
					returnValue = message.getCarrier();
                } else {
                    System.exit(-1);
                }
			}
			socket.close();
		} catch (IOException e) {
			System.err.println("RemoteRef: Connection to server wrong");
			e.printStackTrace();
			System.exit(-1);
		}
		return returnValue;
	}
}
