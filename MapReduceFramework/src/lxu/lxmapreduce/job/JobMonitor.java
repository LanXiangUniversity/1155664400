package lxu.lxmapreduce.job;

import lxu.lxmapreduce.configuration.Configuration;
import lxu.lxmapreduce.configuration.JobConf;
import lxu.lxmapreduce.job.IJobTracker;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Wei on 11/23/14.
 */
public class JobMonitor {


	public static void main(String[] args) throws RemoteException, NotBoundException {

		IJobTracker jobTracker = null;
		Configuration conf = new Configuration();
		Registry registry = null;

		JobConf jobConf = new JobConf(conf);
		String masterAddr = jobConf.getSocketAddr("master.address", "localhost");
		int rmiPort = jobConf.getInt("rmi.port", 1099);
		registry = LocateRegistry.getRegistry(masterAddr, rmiPort);
		jobTracker = (IJobTracker)registry.lookup("JobTracker");
		if (jobTracker == null) {
			System.out.println("Cannot lookup JobTracker");
		}
		System.out.println("LXU Cluster Job Status");

		ConcurrentHashMap<String, JobInProgress> jobs = jobTracker.getJobs();
		for (String job : jobs.keySet()) {
			JobInProgress jip = jobs.get(job);
			int statusMap = jip.getJobStatus().getMapState();
			int statusReduce = jip.getJobStatus().getReduceState();

			String map = "Map tasks: ";
			String reduce = "Reduce tasks: ";

			switch (statusMap) {
				case 1: map += "RUNNING";
					break;
				case 2: map += "SUCCEEDED";
					break;
				case 3: map += "FAILED";
					break;
				case 4: map += "KILLED";
					break;
				default:
					break;
			}

			switch (statusReduce) {
				case 1: reduce += "RUNNING";
					break;
				case 2: reduce += "SUCCEEDED";
					break;
				case 3: reduce += "FAILED";
					break;
				case 4: reduce += "KILLED";
					break;
				default:
					break;
			}

			System.out.println("JobID: " + job + ", " + map + ", " + reduce);
		}
	}
}
