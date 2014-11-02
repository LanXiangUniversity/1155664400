/**
 * RMIRegistry.java
 *
 * @author Tong Wei, Guoli Ma {twei1, guolim}@andrew.cmu.edu
 *
 * This is the simulation for Java's compiled stub. All stub must extend the
 * RemoteStub abstract object. Stub need to call invoke method of RemoteStub
 * to send method name and arguments to remote server.
 */

import myrmi.server.RemoteRef;
import myrmi.server.RemoteStub;

public class TestServer_stub extends RemoteStub implements Hello {
	/**
	 *
	 */
	private static final long serialVersionUID = 7055713594038349921L;

	public TestServer_stub(RemoteRef ref) {
		super(ref);
	}


	public String sayHello(String echo) {
		Object[] args = {echo};
		Object returnValue = invoke("sayHello", args);
		return (String) returnValue;
	}

	public int getX() {
		Object[] args = {};
		Object returnValue = invoke("getX", args);
		return (int) returnValue;
	}

	public int add(Hello another) {
		Object[] args = {another};
		Object returnValue = invoke("add", args);
		return (int) returnValue;
	}
}
