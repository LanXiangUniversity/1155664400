package lxu.lxmapreduce.job;

import lxu.lxmapreduce.metadata.CommitMapAction;
import lxu.lxmapreduce.metadata.TaskTrackerAction;
import lxu.lxmapreduce.metadata.TaskTrackerStatus;
import lxu.lxmapreduce.task.Task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by magl on 14/11/10.
 */
public class TaskScheduler {
	// use job tracker to get job information
	private JobTracker jobTracker = null;

	public void setJobTracker(JobTracker jobTracker) {
		this.jobTracker = jobTracker;
	}

	public synchronized List<Task> assignTasks(TaskTrackerStatus taskTrackerStatus) {
		List<Task> assignedTasks = new ArrayList<Task>();

		// Get map reduce count for current taskTracker
		final int trackerMapCapacity = taskTrackerStatus.getMaxMapTasks();
		final int trackerReduceCapacity = taskTrackerStatus.getMaxReduceTasks();
		final int trackerRunningMaps = taskTrackerStatus.countRunningMapTask();
		final int trackerRunningReduces = taskTrackerStatus.countRunningReduceTask();

        // TODO: Change to listener
		Collection<JobInProgress> jobs = jobTracker.jobs.values();
		JobInProgress job = jobs.iterator().next();

		int remainingMapLoad = job.getNumMapTasks() - job.getRunningMapTasks() - job.getFinishedMapTasks();
		int remainingReduceLoad = job.getNumReduceTasks() - job.getRunningReduceTasks() - job.getFailedReduceTask();

        int mapLoad = Math.min(trackerMapCapacity - trackerRunningMaps, remainingMapLoad);
        int reduceLoad = Math.min(trackerReduceCapacity - trackerRunningReduces, remainingReduceLoad);

		// assign map tasks
		for (int i = 0; i < mapLoad; ++i) {
			Task task = job.obtainNewLocalMapTask(taskTrackerStatus);
			if (task != null) {
				assignedTasks.add(task);
			} else {
				task = job.obtainNewNonLocalMapTask(taskTrackerStatus);
				assignedTasks.add(task);
			}
		}

        // TODO: assign reduce tasks
        // Only assign reduce tasks when map is done
        if (job.getJobStatus().isMapComplete()) {
            System.out.println("should assign reduce task");
            Task task = job.obtainNewReduceTask(taskTrackerStatus);
            if (task != null) {
                assignedTasks.add(task);
                System.out.println("assign reduce task");
            }
        }

		return assignedTasks;
	}
}
