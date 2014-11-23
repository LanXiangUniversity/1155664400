package lxu.lxdfs.namenode;

import lxu.lxdfs.service.INameSystemService;
import lxu.lxdfs.service.NameSystemService;
import lxu.lxmapreduce.job.JobTracker;
import lxu.lxmapreduce.configuration.Configuration;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * NameNode.java
 * Created by Wei on 11/2/14.
 *
 * NameNode serves as both directory namespace manager and "block table" for
 * the lxdfs. There is a single NameNode running in any DFS deployment.
 *
 * All namespace service is handled by {@link lxu.lxdfs.service.NameSystemService}.
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
		System.out.println("NameNode register service");

		nameNode.nameSystem.enterSafeMode();

		nameNode.nameSystem.exitSafeMode();
	}

    /**
     * registerService
     *
     * NameNode register its service using java rmi.
     */
	public void registerService() {
        int rmiPort = configuration.getInt("rmi.port", 1099);
		try {
            // register NameSystemService
			INameSystemService nameSystem = new NameSystemService(configuration);
			registry = LocateRegistry.createRegistry(rmiPort);
            INameSystemService stub =
                    (INameSystemService) UnicastRemoteObject.exportObject(nameSystem, rmiPort);
			registry.rebind("NameSystemService", stub);
			this.nameSystem = nameSystem;
            // register job tracker
            jobTracker = new JobTracker(nameSystem);
            jobTracker.startService(registry, rmiPort);
			System.out.println("NameNode Start!");
		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
