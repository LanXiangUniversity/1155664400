package myrmi.registry;

/**
 * Registry.java
 * 
 * @author Tong Wei, Guoli Ma {twei1, guolim}@andrew.cmu.edu
 * 
 * A simulation for java.rmi.registry.Registry.
 * Registry is an object that provides methods for storing and retrieving remote
 * object references bound with arbitrary string names. The bind, unbind, and 
 * rebind methods are used to alter the name bindings in the registry, and the 
 * lookup and list methods are used to query the current name bindings.
 */

import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;

import myrmi.RemoteException;
import myrmi.util.Communication;
import myrmi.util.RMIMessage;

public final class Registry implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = 516805493490175644L;
	
	public static final int REGISTRY_PORT = 15640;

    private String host;	/* RMIRegistry host name */
    private int port;		/* RMIRegistry listening port */

    /**
     * Constructors.
     */
    public Registry() {
        host = "localhost";
        port = REGISTRY_PORT;
    }

    public Registry(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * lookup - Send lookup message to RMIRegistry.
     * @param name - Service name.
     * @return The stub binded to the service name
     */
	public Object lookup(String name) throws RemoteException {
        Object returnObject = null;
        
        try {
            Socket socket = new Socket(host, port);
            System.out.println("Lookup " + name + 
 				   			   " on " + host + ":" + port);
            RMIMessage message =
                    new RMIMessage()
                        .withMessageType(RMIMessage.MessageType.LOOK_UP)
                        .withName(name);
                
            if (Communication.sendMessage(message, socket) == true) {
                RMIMessage returnMessage = 
                        Communication.receiveMessage(socket);
                
                if (RMIMessage.checkResponse(returnMessage)) {
                    returnObject = returnMessage.getCarrier();
                } else {
                    System.exit(-1);
                }
            }
        } catch (IOException e) {
            System.out.println("Registry: cannot connect to RMIRegistry");
            e.printStackTrace();
            System.exit(-1);
        }

		return returnObject;
	}

	/**
	 * bind - Bind a stub to the specified service name.
	 * @param name - Service name.
	 * @param obj - The stub.
	 */
	public void bind(String name, Object obj) throws RemoteException {
        try {
            Socket socket = new Socket(host, port);
            System.out.println("Bind " + obj.getClass().getName() + 
            				   " with name " + name + " to " + 
            				   host + ":" + port);
            RMIMessage message =
                    new RMIMessage()
                        .withMessageType(RMIMessage.MessageType.BIND)
                        .withName(name)
                        .withCarrier(obj);
                
            if (Communication.sendMessage(message, socket) == true) {
                RMIMessage returnMessage = 
                        Communication.receiveMessage(socket);
                
                if (RMIMessage.checkResponse(returnMessage)) {
                    System.out.println("Bind Success");
                } else {
                    System.exit(-1);
                }
            }
        } catch (IOException e) {
            System.out.println("Registry: cannot connect to RMIRegistry");
            e.printStackTrace();
        }
	}

	/**
	 * unbind - Unbind a service name.
	 * @param name - Service name.
	 */
	public void unbind(String name) throws RemoteException {
        try {
            Socket socket = new Socket(host, port);
            System.out.println("Unbind " + name + 
            				   " on " + host + ":" + port);
            RMIMessage message =
                    new RMIMessage()
                        .withMessageType(RMIMessage.MessageType.UNBIND)
                        .withName(name);
                
            if (Communication.sendMessage(message, socket) == true) {
                RMIMessage returnMessage = 
                        Communication.receiveMessage(socket);
                
                if (RMIMessage.checkResponse(returnMessage)) {
                    System.out.println("Unbind Success");
                } else {
                    System.exit(-1);
                }
            }
        } catch (IOException e) {
            System.out.println("Registry: cannot connect to RMIRegistry");
            e.printStackTrace();
        }
	}

	/**
	 * rebind - Rebind a stub to a new service name.
	 * @param name - New service name.
	 * @param obj - The stub.
	 */
	public void rebind(String name, Object obj) throws RemoteException {
        try {
            Socket socket = new Socket(host, port);
            System.out.println("Rebind " + obj.getClass().getName() + 
 				   			   "with name " + name + " to " + 
 				   			   host + " " + port);
            RMIMessage message =
                    new RMIMessage()
                        .withMessageType(RMIMessage.MessageType.REBIND)
                        .withName(name)
                        .withCarrier(obj);
                
            if (Communication.sendMessage(message, socket) == true) {
                RMIMessage returnMessage = 
                        Communication.receiveMessage(socket);
                
                if (RMIMessage.checkResponse(returnMessage)) {
                    System.out.println("Rebind Success");
                } else {
                    System.exit(-1);
                }
            }
        } catch (IOException e) {
            System.out.println("Registry: cannot connect to RMIRegistry");
            e.printStackTrace();
        }
	}

	/**
	 * list - List all service name of the RMIRegistry
	 * @return A list of service name.
	 * @throws RemoteException
	 */
	public String[] list() throws RemoteException {
		String[] nameList = null;
		
        try {
            Socket socket = new Socket(host, port);

            System.out.println("Send list message to " + host + " " + port);
            
            RMIMessage message =
                    new RMIMessage()
                        .withMessageType(RMIMessage.MessageType.LIST);
                
            if (Communication.sendMessage(message, socket) == true) {
                RMIMessage returnMessage = 
                        Communication.receiveMessage(socket);
                
                if (RMIMessage.checkResponse(returnMessage)) {
                    nameList = ((String)returnMessage.getCarrier()).split(",");
                } else {
                    System.exit(-1);
                }
            }
        } catch (IOException e) {
            System.out.println("Registry: cannot connect to RMIRegistry");
            e.printStackTrace();
        }
        
        return nameList;
	}

}
