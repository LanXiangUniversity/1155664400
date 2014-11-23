package lxu.lxmapreduce.job;

import lxu.lxmapreduce.metadata.TaskTrackerStatus;
import lxu.lxmapreduce.task.Task;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * TaskScheduler.java
 * Created by magl on 14/11/10.
 *
 * This class is a FIFO task scheduler. All jobs are queued in jobQueue.
 * The head of the queue has the highest priority and the tail the lowest.
 * When assigning a task, scan the entire queue. If the head of the queue
 * has un-running task, then assign them. If not, assign tasks of the next
 * job and so on.
 */
public class TaskScheduler {
    // use job tracker to get job information
    private JobTracker jobTracker = null;
    private Queue<String> jobQueue = new LinkedList<String>();

    public void setJobTracker(JobTracker jobTracker) {
        this.jobTracker = jobTracker;
    }

    public void addJobToQueue(String jobID) {
        this.jobQueue.offer(jobID);
    }

    /**
     * assignTasks
     *
     * Scan the entire jobQueue. If the head of the queue has un-running tasks,
     * then assign them. If not, assign tasks of the next job and so on.
     * @param taskTrackerStatus
     * @return
     * @throws RemoteException
     * @throws NotBoundException
     */
    public synchronized List<Task> assignTasks(TaskTrackerStatus taskTrackerStatus) throws RemoteException, NotBoundException {
        List<Task> assignedTasks = new ArrayList<Task>();

        // Get map reduce count for current taskTracker
        final int trackerMapCapacity = taskTrackerStatus.getMaxMapTasks();
        final int trackerReduceCapacity = taskTrackerStatus.getMaxReduceTasks();
        final int trackerRunningMaps = taskTrackerStatus.countRunningMapTask();
        final int trackerRunningReduces = taskTrackerStatus.countRunningReduceTask();

        int remainingMapLoad = 0;
        int remainingReduceLoad = 0;

        /*
         * First calculate remaining map and reduce work load.
         */
        for (String jobID : jobQueue) {
            JobInProgress job = jobTracker.getJobInProgress(jobID);
            if (!job.isComplete()) {
                remainingMapLoad += (job.getNumMapTasks() -
                        job.getRunningMapTasks() -
                        job.getFinishedMapTasks());
                if (job.shouldAssignReduceTask()) {
                    remainingReduceLoad += (job.getNumReduceTasks() -
                            job.getRunningReduceTasks() -
                            job.getFinishedReduceTasks());
                }
            }
        }

        int mapLoad = Math.min(trackerMapCapacity - trackerRunningMaps, remainingMapLoad);
        int reduceLoad = Math.min(trackerReduceCapacity - trackerRunningReduces, remainingReduceLoad);

        // assign map tasks
        for (int i = 0; i < mapLoad; ++i) {
            synchronized (jobQueue) {
                for (String jobID : jobQueue) {
                    JobInProgress job = jobTracker.getJobInProgress(jobID);

                    if (job.isComplete()) {
                        continue;
                    }

                    Task task = job.obtainNewMapTask(taskTrackerStatus);
                    if (task != null) {
                        System.out.println("New map task " + task.getTaskAttemptID() + " assigned");
                        assignedTasks.add(task);
                        break;
                    }
                }
            }
        }

        // Only assign reduce tasks when map is done
        if (reduceLoad > 0) {
            synchronized (jobQueue) {
                for (String jobID : jobQueue) {
                    JobInProgress job = jobTracker.getJobInProgress(jobID);
                    if (job.isComplete() || job.getNumReduceTasks() == 0) {
                        continue;
                    }

                    if (job.getJobStatus().isMapComplete()) {
                        Task task = job.obtainNewReduceTask(taskTrackerStatus);
                        if (task != null) {
                            assignedTasks.add(task);
                            System.out.println("assign new reduce task");
                            break;
                        }
                    }
                }
            }
        }

        return assignedTasks;
    }
}
