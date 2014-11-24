package lxu.lxmapreduce.job;

import lxu.lxmapreduce.metadata.HeartbeatResponse;
import lxu.lxmapreduce.metadata.TaskTrackerStatus;
import lxu.lxmapreduce.configuration.JobConf;

import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * IJobTracker.java
 * Created by magl on 14/11/10.
 *
 * The interface of all JobTracker service.
 */
public interface IJobTracker extends Remote {
	// for job client
	public String getNewJobID() throws RemoteException;

	public JobStatus submitJob(String jobID, JobConf jobConf) throws RemoteException;

    public JobStatus getJobStatus(String jobID) throws RemoteException;

	// for TaskTracker
	public HeartbeatResponse heartbeat(TaskTrackerStatus status,
	                                   boolean initialContact,
	                                   boolean acceptNewTasks,
	                                   short responseID) throws RemoteException, NotBoundException;

	public ConcurrentHashMap<String, JobInProgress> getJobs() throws RemoteException;
}
