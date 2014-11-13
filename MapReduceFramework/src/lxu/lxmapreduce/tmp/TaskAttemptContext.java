package lxu.lxmapreduce.tmp;

import lxu.lxdfs.metadata.LocatedBlock;

/**
 * Created by Wei on 11/12/14.
 */
public class TaskAttemptContext extends JobContext {
	private final TaskID taskId;
	private String status = "";
	private LocatedBlock locatedBlock;

	public TaskAttemptContext(Configuration conf, TaskID taskId) {
		super(conf, taskId.getJobID());
		this.taskId = taskId;
	}

	public TaskID getTaskId() {
		return taskId;
	}

	public LocatedBlock getLocatedBlock() {
		return locatedBlock;
	}

	public void setLocatedBlock(LocatedBlock locatedBlock) {
		this.locatedBlock = locatedBlock;
	}

	public TaskID getTaskAttemptID() {
		return taskId;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String msg) {
		status = msg;
	}
}
