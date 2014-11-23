package lxu.lxmapreduce.job;

import lxu.lxmapreduce.tmp.Configuration;
import lxu.lxmapreduce.tmp.JobConf;
import lxu.lxmapreduce.tmp.JobContext;

import java.io.IOException;

/**
 * Created by magl on 14/11/15.
 */
public class Job extends JobContext {
    private JobClient jobClient;
    private JobState state = JobState.DEFINE;
    private JobStatus status = null;

    public static enum JobState {
        DEFINE, RUNNING
    }

    public Job(JobConf conf) {
        super(conf, null);
        jobClient = new JobClient(conf);
    }

    public void updateStatus() {
        this.status = jobClient.updateStatus(status.getJobID());
    }

    public JobStatus getStatus() throws IOException, InterruptedException {
        ensureState(JobState.RUNNING);
        updateStatus();
        return status;
    }

    public void setNumMapTasks(int numMapTasks) throws IllegalStateException {
        ensureState(JobState.DEFINE);
        conf.setNumMapTasks(numMapTasks);
    }

    public void setNumReduceTasks(int numReduceTasks) throws IllegalStateException {
        ensureState(JobState.DEFINE);
        conf.setNumReduceTasks(numReduceTasks);
    }

    public void setInputFormatClass(Class<?> inputFormatClass) {
        ensureState(JobState.DEFINE);
        conf.setInputFormatClass(inputFormatClass);
    }

    public void setOutputFormatClass(Class<?> outputFormatClass) {
        ensureState(JobState.DEFINE);
        conf.setOutputFormatClass(outputFormatClass);
    }

    public void setMapperClass(Class<?> mapperClass) {
        ensureState(JobState.DEFINE);
        conf.setMapperClass(mapperClass);
    }

    public void setReducerClass(Class<?> reducerClass) {
        ensureState(JobState.DEFINE);
        conf.setReducerClass(reducerClass);
    }

    public void setMapOutputKeyClass(Class<?> mapOutputKeyClass) {
        ensureState(JobState.DEFINE);
        conf.setMapOutputKeyClass(mapOutputKeyClass);
    }

    public void setMapOutputValueClass(Class<?> mapOutputValueClass) {
        ensureState(JobState.DEFINE);
        conf.setMapOutputValueClass(mapOutputValueClass);
    }

    public void setReduceOutputKeyClass(Class<?> reduceOutputKeyClass) {
        ensureState(JobState.DEFINE);
        conf.setMapOutputKeyClass(reduceOutputKeyClass);
    }

    public void setReduceOutputValueClass(Class<?> reduceOutputValueClass) {
        ensureState(JobState.DEFINE);
        conf.setReduceOutputValueClass(reduceOutputValueClass);
    }

    public void setOutputKeyClass(Class<?> outputKeyClass) {
        ensureState(JobState.DEFINE);
        conf.setOutputKeyClass(outputKeyClass);
    }

    public void setOutputValueClass(Class<?> outputValueClass) {
        ensureState(JobState.DEFINE);
        conf.setOutputValueClass(outputValueClass);
    }

    public void setJobName(String name) {
        ensureState(JobState.DEFINE);
        conf.setJobName(name);
    }

	public void setJarName(String name) {
		ensureState(JobState.DEFINE);
		conf.setJarName(name);
	}

    public String getInputPath() {
        return conf.getInputPath();
    }

    public void setInputPath(String path) {
        ensureState(JobState.DEFINE);
        conf.setInputPath(path);
    }

    public String getOutputPath() {
        return conf.getOutputPath();
    }

    public void setOutputPath(String path) {
        ensureState(JobState.DEFINE);
        conf.setOutputPath(path);
    }

    public boolean isComplete() {
        ensureState(JobState.RUNNING);
        updateStatus();
        return status.isJobComplete();
    }

    public boolean isSuccessful() {
        ensureState(JobState.RUNNING);
        updateStatus();
        return status.isSuccessful();
    }

    public void submit() {
        if (getInputPath() == null) {
            System.err.println("FATAL: Missing input file(s)!");
            System.exit(-1);
        }
        status = jobClient.submitJob(this, new JobConf(getConfiguration()));
        if (status == null) {
            System.err.println("Error: Job submitting wrong");
            System.exit(-1);
        }
        state = JobState.RUNNING;
    }

    public boolean waitForCompletion() {
        if (state == JobState.DEFINE) {
            submit();
        }

        while (!isComplete()) {
            System.out.println(String.format("Map : %5.2f%% Reduce : %5.2f%%",
                               status.getMapProgress(),
                               status.getReduceProgress()));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {}
        }
        System.out.println(String.format("Map : %5.2f%% Reduce : %5.2f%%",
                status.getMapProgress(),
                status.getReduceProgress()));

        if (status.isMapSuccessful() && status.isReduceSuccessful()) {
            System.out.println("Job " + getJobName() + " succeed");
        } else {
            System.out.println("Job " + getJobName() + " failed");
        }
        return isSuccessful();
    }

    private void ensureState(JobState state) throws IllegalStateException {
        if (this.state != state) {
            throw new IllegalStateException("Job in state "+ this.state +
                                            " instead of " + state);
        }

        if (state == JobState.RUNNING && jobClient == null) {
            throw new IllegalStateException("Job in state " + JobState.RUNNING +
                                            " however jobClient is not initialized!");
        }
    }
}
