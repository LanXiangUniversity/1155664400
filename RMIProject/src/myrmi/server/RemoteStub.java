package myrmi.server;

/**
 * RemoteStub.java
 * 
 * @author Tong Wei, Guoli Ma {twei1, guolim}@andrew.cmu.edu
 * 
 * A simulation of java.rmi.server.RemoteStub.
 * It is the superclass of all remote object stub. It wraps the invoke method 
 * of its RemoteRef. All subclasses should call the wrapper invoke method to 
 * send remote method invoke message to its proxy.
 */


import java.io.Serializable;

import myrmi.Remote;

public abstract class RemoteStub implements Serializable, Remote {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8298425839228532605L;

	protected RemoteRef remoteObjectReference;
	
	public RemoteStub(RemoteRef ref) {
		this.remoteObjectReference = ref;
	}
	
	public RemoteRef getRemoteRef() {
		return this.remoteObjectReference;
	}
	
	public Object invoke(String methodName, Object[] args) {
		return this.remoteObjectReference.invoke(methodName, args);
	}
}
