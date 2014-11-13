package lxu.lxmapreduce.tmp;

/**
 * A read-only view of the job that is provided to the tasks while they
 * are running.
 * Created by Wei on 11/12/14.
 */
public class JobContext {

	protected static final String INPUT_FORMAT_CLASS_ATTR =
			"mapreduce.inputformat.class";
	protected static final String MAP_CLASS_ATTR = "mapreduce.map.class";
	protected static final String COMBINE_CLASS_ATTR = "mapreduce.combine.class";
	protected static final String REDUCE_CLASS_ATTR = "mapreduce.reduce.class";
	protected static final String OUTPUT_FORMAT_CLASS_ATTR =
			"mapreduce.outputformat.class";
	protected static final String PARTITIONER_CLASS_ATTR =
			"mapreduce.partitioner.class";
	protected final JobConf conf;
	private final String jobId;

	public JobContext(JobConf conf, String jobId) {
		this.conf = conf;
		this.jobId = jobId;
	}

	public JobContext(Configuration conf, String jobId) {
		this.conf = new JobConf(conf);
		this.jobId = jobId;
	}

	public Configuration getConfiguration() {
		return conf;
	}


	public int getNumReduceTasks() {
		return conf.getNumReduceTasks();
	}


	public String getJobId() {
		return this.jobId;
	}

	public Class<?> getOutputKeyClass() {
		return conf.getOutputKeyClass();
	}

	public Class<?> getOutputValueClass() {
		return conf.getOutputValueClass();
	}

	public Class<?> getMapOutputKeyClass() {
		return conf.getMapOutputKeyClass();
	}

	public Class<?> getMapOutputValueClass() {
		return conf.getMapOutputValueClass();
	}

	public String getJobName() {
		return conf.getJobName();
	}

	public Class<?> getInputFormatClass()
			throws ClassNotFoundException {
		return (Class<?>) conf.getClass(INPUT_FORMAT_CLASS_ATTR);
	}

	public Class<?> getMapperClass() throws ClassNotFoundException {
		return (Class<?>) conf.getClass(MAP_CLASS_ATTR);
	}

	public Class<?> getOutputFormatClass() throws ClassNotFoundException {
		return (Class<?>) conf.getClass(OUTPUT_FORMAT_CLASS_ATTR);
	}

	public String getJar() {
		return conf.getJar();
	}
}
