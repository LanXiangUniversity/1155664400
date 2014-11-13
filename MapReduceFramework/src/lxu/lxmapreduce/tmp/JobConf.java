package lxu.lxmapreduce.tmp;

import lxu.lxmapreduce.io.format.InputFormat;
import lxu.utils.ReflectionUtils;

/**
 * Created by Wei on 11/12/14.
 */
public class JobConf extends Configuration {
	protected int jobId;
	protected String jarName;
	protected int numMapTasks;
	protected int numReduceTasks;
	protected int maxAttempts;
	protected String jobName;
	protected String inputFormat;
	protected String mapClassName;
	protected String reduceClassName;
	protected String outputFormatName;
	protected String inputFormatName;

	public JobConf(Configuration conf) {
		super(conf);
	}

	public Class<?> getMapperClass() throws ClassNotFoundException {
		return getClassByName(this.mapClassName);
	}

	public void setMapperClass(Class<?> theClass) {
		this.mapClassName = theClass.getName();
	}

	public Class<?> getReducerClass() throws ClassNotFoundException {
		return getClassByName(this.reduceClassName);
	}

	public void setReducerClass(Class<?> theClass) {
		this.reduceClassName = theClass.getName();
	}

	public OutputFormat getOutputFormat() throws IllegalAccessException, ClassNotFoundException, InstantiationException {
		return ReflectionUtils.newInstance(this.outputFormatName);
	}

	public void setOutputFormat(Class<? extends OutputFormat> theClass) {
		this.outputFormatName = theClass.getName();
	}

	public InputFormat getInputFormat() throws IllegalAccessException, ClassNotFoundException, InstantiationException {
		return ReflectionUtils.newInstance(this.inputFormatName);
	}

	public void setInputFormat(Class<? extends InputFormat> theClass) {
		this.inputFormatName = theClass.getName();
	}

	public String getJar() {
		return this.jarName;
	}

	public void setJar(String jarName) {
		this.jarName = jarName;
	}

	public Class<?> getOutputValueClass() {
		return null;
	}

	public Class<?> getOutputKeyClass() {
		return null;
	}

	public Class<?> getInputputFormatClass() {
		return null;
	}

	public Class<?> getOutputFormatClass() {
		return null;
	}

	public Class<?> getMapOutputKeyClass() {
		return null;
	}

	public Class<?> getMapOutputValueClass() {
		return null;
	}

	public String getJobName() {
		return null;
	}

	public int getNumReduceTasks() {
		return 0;
	}
}


