package lxu.lxmapreduce.job;

import lxu.lxmapreduce.tmp.JobConf;
import lxu.lxmapreduce.tmp.JobContext;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Created by Wei on 11/10/14.
 */
public class JobClient {
    private JobConf jobConf;
    private IJobTracker jobTracker;

    public JobClient() {
        this.jobConf = new JobConf();
        locateJobTracker();
    }

    public JobClient(JobConf jobConf) {
        this.jobConf = jobConf;
        locateJobTracker();
    }

    public void locateJobTracker() {
        Registry registry = null;
        try {
            registry = LocateRegistry.getRegistry();
            jobTracker = (IJobTracker)registry.lookup("JobTracker");
            if (jobTracker == null) {
                System.out.println("Cannot lookup JobTracker");
            }
        } catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
            System.err.println("Error: JobClient getting JobTracker wrong!");
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }

    public JobStatus updateStatus(String jobID) {
        if (jobTracker == null) {
            System.err.println("Error: JobClient cannot connect to JobTracker!");
            return null;
        }
        JobStatus newStatus = null;
        try {
            newStatus = jobTracker.getJobStatus(jobID);
        } catch (RemoteException e) {
            System.err.println("Error: JobClient fetching JobStatus wrong!");
            System.err.println(e.getMessage());
            return null;
        }
        return newStatus;
    }

    public JobStatus submitJob(Job job, JobConf jobConf) {
        JobStatus jobStatus = null;
        try {
            String jobID = jobTracker.getNewJobID();
            job.setJobId(jobID);
            jobStatus = jobTracker.submitJob(jobID, jobConf);
        } catch (RemoteException e) {
            System.err.println("Error: JobClient submitting Job wrong!");
            e.printStackTrace();
            return null;
        }
        return jobStatus;
    }
}
