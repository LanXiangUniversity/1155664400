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
	private String jobFile;                         // job configuration file
	private TaskID taskId;                          // unique, includes job id
	TaskStatus taskStatus;                          // current status of the task
	private int partition;                          // id within job

	protected JobConf conf;
	protected JobContext jobContext;
	protected TaskAttemptContext taskContext;



	/** Construct output file names so that, when an output directory listing is
	 * sorted lexicographically, positions correspond to output partitions.*/
	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
	static {
		NUMBER_FORMAT.setMinimumIntegerDigits(5);
		NUMBER_FORMAT.setGroupingUsed(false);
	}

	public void setJobFile(String jobFile) { this.jobFile = jobFile; }
	public String getJobFile() { return jobFile; }
	public TaskID getTaskID() { return taskId; }

	static synchronized String getOutputName(int partition) {
		return "part-" + NUMBER_FORMAT.format(partition);
	}

	public abstract void run(JobConf jobConf) throws IOException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException, IllegalAccessException;
}
