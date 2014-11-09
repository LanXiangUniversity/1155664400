package registry;

import org.jetbrains.annotations.Nullable;
import remote.RemoteObjectRef;

import java.io.*;
import java.util.Hashtable;

/**
 * Created by Wei on 10/3/14.
 */

// Create a local reference to the registry.
public class FileRegistry {
	// Registry holds its port and host, connects to it each time.
	private String path;

	public FileRegistry(String path) {
		this.path = path;
	}

	// Return the ROR (if found) or null (if else)
	public RemoteObjectRef lookup(String serviceName) throws IOException, ClassNotFoundException {
		File file = new File(this.path);
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
		RemoteObjectRef rorTmp = null;
		RemoteObjectRef ror = null;

		Hashtable table = (Hashtable) in.readObject();

		if (table.containsKey(serviceName)) {
			System.out.println("ROR for " + serviceName + " found.");
			rorTmp = (RemoteObjectRef) table.get(serviceName);
		} else {
			System.out.println("ROR not found");
		}

		ror = new RemoteObjectRef(
				rorTmp != null ? rorTmp.getIPAddress() : null,
				rorTmp != null ? rorTmp.getPort() : 0,
				rorTmp != null ? rorTmp.getObjKey() : 0,
				rorTmp != null ? rorTmp.getRemoteInterfaceName() : null
		);

		in.close();
		return ror;
	}

	// Rebind a ROR (can be null).
	public void rebind(String serviceName, RemoteObjectRef ror) throws IOException, ClassNotFoundException {
		ObjectOutputStream out;
		ObjectInputStream in;
		Hashtable table = null;

		File file = new File(this.path);
		if (file.exists()) {
			in = new ObjectInputStream(new FileInputStream(new File(this.path)));
			table = (Hashtable) in.readObject();
			in.close();
		} else {
			table = new Hashtable();
		}

		table.remove(serviceName);
		table.put(serviceName, ror);

		out = new ObjectOutputStream(new FileOutputStream(new File(this.path)));
		out.writeObject(table);

		System.out.println("rebind \"" + serviceName + "\" success");

		out.close();
	}
}
