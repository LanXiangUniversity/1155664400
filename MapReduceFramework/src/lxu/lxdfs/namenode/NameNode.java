package lxu.lxdfs.namenode;

import lxu.lxdfs.service.INameSystemService;
import lxu.lxdfs.service.NameSystemService;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * Created by Wei on 11/2/14.
 */
public class NameNode {
	private INameSystemService nameSystem;

	public static void main(String[] args) throws RemoteException {
		NameNode nameNode = new NameNode();

		// Regsiter and start RPC service.
		nameNode.registerService();
		System.out.println("register service");

		nameNode.nameSystem.enterSafeMode();

		/* TODO enter safe mode. */
		/* TODO reconstruct NameSystem. */
		/* TODO exit safe mode. */

		nameNode.nameSystem.exitSafeMode();


	}

	public void registerService() {
		//if (System.getSecurityManager() == null) {
		//	System.setSecurityManager(new RMISecurityManager());
		//}
		try {
			INameSystemService nameSystem = new NameSystemService();
			//Naming.rebind("rmi://localhost:56789/NameSystemService", nameSystem);
			INameSystemService stub =
					(INameSystemService) UnicastRemoteObject.exportObject(nameSystem, 0);
			Registry registry = LocateRegistry.getRegistry();
			registry.rebind("NameSystemService", stub);
			this.nameSystem = nameSystem;
			System.out.println("NameNode Start!");
		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
