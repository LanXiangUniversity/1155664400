package lxu.lxmapreduce.tmp;

/**
 * A read-only view of the job that is provided to the tasks while they
 * are running.
 * Created by Wei on 11/12/14.
 */
public class JobContext {
    public static final String INPUT_FORMAT_CLASS_ATTR = "mapreduce.job.inputformat.class";
    public static final String MAP_CLASS_ATTR = "mapreduce.job.map.class";
    public static final String REDUCE_CLASS_ATTR = "mapreduce.job.reduce.class";
    public static final String OUTPUT_FORMAT_CLASS_ATTR = "mapreduce.job.outputformat.class";
    public static final String JAR = "mapreduce.job.jar";
    public static final String JOB_NAME = "mapreduce.job.name";
    public static final String NUM_REDUCES = "mapreduce.reduce.number";
    public static final String OUTPUT_KEY_CLASS = "mapreduce.job.output.key.class";
    public static final String OUTPUT_VALUE_CLASS = "mapreduce.job.output.value.class";

    protected final JobConf conf;
    private String jobId;

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


    public String getJobId() {
        return this.jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJar() {
        return conf.getJar();
    }

    public Class<?> getMapperClass() {
        return conf.getMapperClass();
    }

    public Class<?> getReducerClass() {
        return conf.getReducerClass();
    }

    public int getNumMapTasks() {
        return conf.getNumMapTasks();
    }

    public int getNumReduceTasks() {
        return conf.getNumReduceTasks();
    }

    public String getJobName() {
        return conf.getJobName();
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

    public Class<?> getReduceOutputKeyClass() {
        return conf.getReduceOutputKeyClass();
    }

    public Class<?> getReduceOutputValueClass() {
        return conf.getReduceOutputValueClass();
    }

    public Class<?> getInputFormatClass() {
        return conf.getInputFormatClass();
    }

    public Class<?> getOutputFormatClass() {
        return conf.getOutputFormatClass();
    }
}
