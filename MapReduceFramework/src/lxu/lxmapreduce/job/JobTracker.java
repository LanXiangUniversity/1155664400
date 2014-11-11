package lxu.lxmapreduce.job;

import lxu.lxmapreduce.metadata.HeartbeatResponse;
import lxu.lxmapreduce.metadata.TaskTrackerStatus;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

/**
 * Created by magl on 14/11/10.
 */
public class JobTracker implements IJobTracker {
    private int nextJobID = 0;
    private TaskScheduler taskScheduler = null;
    private HashMap<Integer, JobInProgress> jobs;

    public JobTracker() {
        this.jobs = new HashMap<Integer, JobInProgress>();
        this.taskScheduler = new TaskScheduler();
    }

    @Override
    public int getNewJobID() {
        return this.nextJobID++;
    }

    @Override
    public JobStatus submitJob(int jobID) {
        if (jobs.containsKey(jobID)) {
            return jobs.get(jobID).getJobStatus();
        }

        JobInProgress job = new JobInProgress(jobID, this);

        jobs.put(jobID, job);

        // TODO: add job to taskScheduler Listener

        return job.getJobStatus();
    }

    @Override
    public HeartbeatResponse heartbeat(TaskTrackerStatus status, short responseID) {
        // TODO: Update TaskTracker, Job, Task information
        // TODO: Generate HeartbeatResponse (including task tracker action)
        // Change heartbeat interval (may not be implemented)
        return null;
    }

    public void startService() {
        // set job tracker of task scheduler
        this.taskScheduler.setJobTracker(this);

        try {
            IJobTracker stub =
                    (IJobTracker) UnicastRemoteObject.exportObject(this, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind("JobTracker", stub);
        } catch (RemoteException e) {
            System.err.println("Error: JobTracker start RMI service error");
            System.err.println(e.getMessage());
        }
    }

    public static void main(String[] args) {
        JobTracker jobTracker = new JobTracker();
        jobTracker.startService();
    }
}
