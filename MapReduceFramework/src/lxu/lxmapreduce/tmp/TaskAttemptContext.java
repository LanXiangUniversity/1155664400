package lxu.lxmapreduce.tmp;

import java.io.IOException;

/**
 * Created by Wei on 11/12/14.
 */
public class TaskAttemptContext extends JobContext {
	private String status = "";
	private final TaskID taskId;

	public TaskAttemptContext(Configuration conf, TaskID taskId) {
		super(conf, taskId.getJobId());
		this.taskId = taskId;
	}

	public TaskID getTaskAttemptID() {
		return taskId;
	}

	public void setStatus(String msg) {
		status = msg;
	}

	public String getStatus() {
		return status;
	}
}
