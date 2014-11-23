package lxu.lxmapreduce.task;

import lxu.lxdfs.metadata.LocatedBlock;
import lxu.lxmapreduce.configuration.JobConf;
import lxu.lxmapreduce.configuration.JobContext;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.rmi.NotBoundException;
import java.text.NumberFormat;

/**
 * Task.java
 * Created by magl on 14/11/10.
 *
 * The base class of all map and reduce task.
 */
public abstract class Task implements Serializable {
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
    protected TaskStatus taskStatus;                          // current status of the task
    protected String jobFile;                         // job configuration file
    protected TaskAttemptID taskAttemptID;                          // unique, includes job id
    protected int partition;                          // id within job
    protected LocatedBlock mapTaskBlock;

    protected Task(TaskAttemptID attemptID, int partition, LocatedBlock locatedBlock) {
        this.taskAttemptID = attemptID;
        this.partition = partition;
        this.mapTaskBlock = locatedBlock;
    }

    static synchronized String getOutputName(int partition) {
        return "part-" + NUMBER_FORMAT.format(partition);
    }

    public String getJobFile() {
        return jobFile;
    }

    public void setJobFile(String jobFile) {
        this.jobFile = jobFile;
    }

    public TaskAttemptID getTaskAttemptID() {
        return taskAttemptID;
    }

    public LocatedBlock getMapTaskBlock() {
        return mapTaskBlock;
    }

    public abstract boolean isMapTask();

    public JobConf getConf() {
        return conf;
    }

    public void setConf(JobConf conf) {
        this.conf = conf;
    }

    public abstract void initialize();

    public abstract void run(JobConf jobConf) throws IOException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException, IllegalAccessException, NotBoundException;
}
