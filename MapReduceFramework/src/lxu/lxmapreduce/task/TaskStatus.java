package lxu.lxmapreduce.task;

import lxu.lxmapreduce.tmp.TaskID;

/**
 * Created by magl on 14/11/11.
 */
public abstract class TaskStatus {
	public static final int RUNNING = 1;
	public static final int SUCCEEDED = 2;
	public static final int FAILED = 3;

	private String JobID;
	private String taskTracker;
	private TaskAttemptID taskID;
	private int state;
	private int attemptFailedTime;

	public abstract boolean isMapTask();

	public String getJobID() {
		return JobID;
	}

	public void setJobID(String jobID) {
		JobID = jobID;
	}

	public TaskAttemptID getTaskID() {
		return taskID;
	}

	public void setTaskID(TaskAttemptID taskID) {
		this.taskID = taskID;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	public String getTaskTracker() {
		return taskTracker;
	}

	public void setTaskTracker(String taskTracker) {
		this.taskTracker = taskTracker;
	}

	public void update(int state) {
		setState(state);
	}
}
