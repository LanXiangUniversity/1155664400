package lxu.lxmapreduce.job;

import lxu.lxmapreduce.metadata.HeartbeatResponse;
import lxu.lxmapreduce.metadata.TaskTrackerStatus;
import lxu.lxmapreduce.task.Configuration;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by magl on 14/11/10.
 */
public interface IJobTracker extends Remote {
    // for job client
    // TODO: change prototype
    public String getNewJobID() throws RemoteException;
    public JobStatus submitJob(String jobID, Configuration jobConf) throws RemoteException;

    // for TaskTracker
    public HeartbeatResponse heartbeat(TaskTrackerStatus status,
                                       boolean initialContact,
                                       boolean acceptNewTasks,
                                       short responseID) throws RemoteException;
}
