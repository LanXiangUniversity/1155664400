package lxu.lxmapreduce.job;

import lxu.lxmapreduce.metadata.HeartbeatResponse;
import lxu.lxmapreduce.metadata.TaskTrackerStatus;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by magl on 14/11/10.
 */
public interface IJobTracker extends Remote {
    // for job client
    // TODO: change prototype
    public void submitJob();

    // for TaskTracker
    public HeartbeatResponse heartbeat(TaskTrackerStatus status, short responseID) throws RemoteException;
}
