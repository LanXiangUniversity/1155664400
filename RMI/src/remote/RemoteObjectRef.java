package remote;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by Wei on 10/3/14.
 */
public class RemoteObjectRef implements Serializable {
	// Connection to server
	private String IPAddress;
	private int port;
	// Info about remote object
	private String remoteInterfaceName;
	private int objKey;

	public RemoteObjectRef(String IPAddress, int port, int objKey, String remoteInterfaceName) {
		this.IPAddress = IPAddress;
		this.port = port;
		this.objKey = objKey;
		this.remoteInterfaceName = remoteInterfaceName;
	}

	public String getIPAddress() {
		return IPAddress;
	}

	public void setIPAddress(String IPAddress) {
		this.IPAddress = IPAddress;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getObjKey() {
		return objKey;
	}

	public void setObjKey(int objKey) {
		this.objKey = objKey;
	}

	public String getRemoteInterfaceName() {
		return remoteInterfaceName;
	}

	public void setRemoteInterfaceName(String remoteInterfaceName) {
		this.remoteInterfaceName = remoteInterfaceName;
	}

	// Create a new stub and return it.
	public Object localise() throws ClassNotFoundException, IllegalAccessException, InstantiationException, InvocationTargetException {
		// If the stub class name is remoteInterfaceName+"_stub,
		// then create a new stub.
		// *** Stub should have a constructor without arguments.
		Object rtnObj = null;

		// Init a ROR stub by passing a ROR.
		Class c = Class.forName(this.remoteInterfaceName + "_stub");
		Constructor<?>[] cons = c.getConstructors();
		for (Constructor<?> con : cons) {
			Class<?>[] types = con.getParameterTypes();
			if (types.length == 1 && types[0] == this.getClass()) {
				rtnObj = con.newInstance(this);
				System.out.println("Init a stub.");
			} else {
				System.out.println("Can't find stub's constructor.");
			}
		}

		return rtnObj;
	}

}
