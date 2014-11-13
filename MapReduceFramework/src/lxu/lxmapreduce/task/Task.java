package lxu.lxmapreduce.task;

import lxu.lxmapreduce.tmp.JobConf;
import lxu.lxmapreduce.tmp.JobContext;
import lxu.lxmapreduce.tmp.TaskAttemptContext;
import lxu.lxmapreduce.tmp.TaskID;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.NumberFormat;

/**
 * Created by magl on 14/11/10.
 */
public abstract class Task {
	/**
	 * Construct output file names so that, when an output directory listing is
	 * sorted lexicographically, positions correspond to output partitions.
	 */
	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

	static {
		NUMBER_FORMAT.setMinimumIntegerDigits(5);
		NUMBER_FORMAT.setGroupingUsed(false);
	}

	protected JobConf conf;
	protected JobContext jobContext;
	protected TaskAttemptContext taskContext;
	TaskStatus taskStatus;                          // current status of the task
	private String jobFile;                         // job configuration file
	private TaskID taskId;                          // unique, includes job id
	private int partition;                          // id within job

	static synchronized String getOutputName(int partition) {
		return "part-" + NUMBER_FORMAT.format(partition);
	}

	public String getJobFile() {
		return jobFile;
	}

	public void setJobFile(String jobFile) {
		this.jobFile = jobFile;
	}

	public TaskID getTaskID() {
		return taskId;
	}

	public abstract void run(JobConf jobConf) throws IOException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException, IllegalAccessException;
}
