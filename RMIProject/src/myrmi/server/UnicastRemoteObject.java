package myrmi.server;

/**
 * UnicastRemoteObject.java
 * 
 * @author Tong Wei, Guoli Ma {twei1, guolim}@andrew.cmu.edu
 * 
 * A simulation of java.rmi.server.UnicastRemoteObject.
 * It calls the generateStub method of Proxy to generate a stub for a remote
 * object and export the object to its user.
 */

import java.io.Serializable;

import myrmi.Remote;
import myrmi.registry.Registry;

public class UnicastRemoteObject implements Serializable, Remote {
	
	private static int port = Registry.REGISTRY_PORT;
	/**
	 * 
	 */
	private static final long serialVersionUID = -5580907174782638300L;
	
	protected UnicastRemoteObject() {
		this(Registry.REGISTRY_PORT);
	}
	
	protected UnicastRemoteObject(int p) {
		port = p;
		exportObject((Remote)this, port);
	}

	/**
	 * exportObject - Export a remote stub object of the given remote object 
	 * 				  to user, registry port is default port.
	 * @param impl - A remote object that implements the Remote interface.
	 * @return The stub for the remote object.
	 */
	public static Object exportObject(Remote impl) {
		return exportObject(impl, port);
	}
	
	/**
	 * exportObject - Export a remote stub object of the given remote object 
	 * 				  to user.
	 * @param impl - A remote object that implements the Remote interface.
	 * @param port - The listening port of RMIRegistry.
	 * @return The stub for the remote object.
	 */
	public static Object exportObject(Remote impl, int port) {
        Object stubInstance = null;
        try {
            stubInstance = Proxy.generateStub(impl);
        } catch (ClassNotFoundException e) {
            System.out.println("Object does not implement Remote interface: " +
                               impl.getClass().getName());
            System.exit(-1);
        }
        
        return stubInstance;
	}
}
