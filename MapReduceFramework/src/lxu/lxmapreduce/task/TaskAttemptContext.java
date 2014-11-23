package lxu.lxmapreduce.task;

import lxu.lxdfs.metadata.LocatedBlock;
import lxu.lxmapreduce.configuration.Configuration;
import lxu.lxmapreduce.configuration.JobContext;

/**
 * TaskAttemptContext.java
 * Created by Wei on 11/12/14.
 *
 * The context of a task attempt.
 */
public class TaskAttemptContext extends JobContext {
    private final TaskAttemptID taskId;
    private String status = "";
    private LocatedBlock locatedBlock;

    public TaskAttemptContext(Configuration conf, TaskAttemptID taskId) {
        super(conf, taskId.getJobID());
        this.taskId = taskId;
    }

    public TaskAttemptID getTaskId() {
        return taskId;
    }

    public LocatedBlock getLocatedBlock() {
        return locatedBlock;
    }

    public void setLocatedBlock(LocatedBlock locatedBlock) {
        this.locatedBlock = locatedBlock;
    }

    public TaskAttemptID getTaskAttemptID() {
        return taskId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String msg) {
        status = msg;
    }
}
