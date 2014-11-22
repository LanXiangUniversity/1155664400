package lxu.lxdfs.namenode;

import lxu.lxdfs.service.INameSystemService;
import lxu.lxdfs.service.NameSystemService;
import lxu.lxmapreduce.job.JobTracker;
import lxu.lxmapreduce.tmp.Configuration;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * Created by Wei on 11/2/14.
 */
public class NameNode {
	private INameSystemService nameSystem;
    private Configuration configuration;
    private static Registry registry;
    private JobTracker jobTracker;

    public NameNode() throws Exception {
        configuration = new Configuration();
    }

	public static void main(String[] args) throws Exception {
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
        int rmiPort = configuration.getInt("rmi.port", 1099);
		try {
			INameSystemService nameSystem = new NameSystemService(configuration);
			registry = LocateRegistry.createRegistry(rmiPort);
            INameSystemService stub =
                    (INameSystemService) UnicastRemoteObject.exportObject(nameSystem, rmiPort);
			registry.rebind("NameSystemService", stub);
			this.nameSystem = nameSystem;
            jobTracker = new JobTracker(nameSystem);
            jobTracker.startService(registry, rmiPort);
			System.out.println("NameNode Start!");
		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
