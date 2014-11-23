package lxu.lxmapreduce.tmp;

import lxu.lxmapreduce.io.format.LongWritable;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.task.map.Mapper;
import lxu.lxmapreduce.task.reduce.Reducer;

import java.io.Serializable;

/**
 * Created by Wei on 11/12/14.
 */
public class JobConf extends Configuration implements Serializable {
    private static final long serialVersionUID = 1L;
    public JobConf() {
        super();
    }

    public JobConf(Configuration conf) {
		super(conf);
	}

    public String getJar() {
        return get("mapreduce.job.jar");
    }

    public Class<?> getMapperClass() {
        String name = "mapreduce.map.class";
        return getMapperClass(name);
    }

    public void setMapperClass(Class<?> mapperClass) {
        setClass("mapreduce.map.class", mapperClass);
    }

	public String[] getSocketAddrs() {
		String name = "slave.address";

		return getSocketAddrs(name);
	}

    public Class<?> getMapperClass(String name) {
        return getClass(name, Mapper.class);
    }

    public Class<?> getReducerClass() {
        String name = "mapreduce.reduce.class";
        return getReducerClass(name);
    }

    public void setReducerClass(Class<?> reducerClass) {
        setClass("mapreduce.reduce.class", reducerClass);
    }

    public Class<?> getReducerClass(String name) {
        return getClass(name, Reducer.class);
    }

    public int getNumMapTasks() {
        return getInt("mapreduce.mapper.number", 10);
    }

    public void setNumMapTasks(int numMapTasks) {
        setInt("mapreduce.mapper.number", numMapTasks);
    }

    public int getNumReduceTasks() {
        return getInt("mapreduce.reducer.number", 10);
    }

    public void setNumReduceTasks(int numReduceTasks) {
        setInt("mapreduce.reducer.number", numReduceTasks);
    }

    public String getJobName() {
        return get("mapreduce.job.name");
    }

    public void setJobName(String jobName) {
        set("mapreduce.job.name", jobName);
    }

	public void setJarName(String jobName) {
		set("mapreduce.jar.name", jobName);
	}

	public String getJarName() {
		return get("mapreduce.jar.name");
	}


	public Class<?> getOutputKeyClass() {
        return getClass("mapreduce.output.key.class", LongWritable.class);
    }

    public void setOutputKeyClass(Class<?> outputKeyClass) {
        setClass("mapreduce.output.key.class", outputKeyClass);
    }

    public Class<?> getOutputValueClass() {
        return getClass("mapreduce.output.value.class", Text.class);
    }

    public void setOutputValueClass(Class<?> outputValueClass) {
        setClass("mapreduce.output.value.class", outputValueClass);
    }

    public Class<?> getMapOutputKeyClass() {
        return getClass("mapreduce.map.output.key.class", LongWritable.class);
    }

    public void setMapOutputKeyClass(Class<?> mapOutputKeyClass) {
        setClass("mapreduce.map.output.key.class", mapOutputKeyClass);
    }

    public Class<?> getMapOutputValueClass() {
        return getClass("mapreduce.map.output.value.class", Object.class);
    }

    public void setMapOutputValueClass(Class<?> mapOutputValueClass) {
        setClass("mapreduce.map.output.value.class", mapOutputValueClass);
    }

    public Class<?> getReduceOutputKeyClass() {
        return getClass("mapreduce.reduce.output.key.class", LongWritable.class);
    }

    public void setReduceOutputKeyClass(Class<?> reduceOutputKeyClass) {
        setClass("mapreduce.reduce.output.key.class", reduceOutputKeyClass);
    }

    public Class<?> getReduceOutputValueClass() {
        return getClass("mapreduce.reduce.output.value.class", Object.class);
    }

    public void setReduceOutputValueClass(Class<?> reduceOutputValueClass) {
        setClass("mapreduce.reduce.output.value.class", reduceOutputValueClass);
    }

    public Class<?> getInputFormatClass() {
        return getClass("mapreduce.inputformat.class", Text.class);
    }

    public void setInputFormatClass(Class<?> inputFormatClass) {
        setClass("mapreduce.inputformat.class", inputFormatClass);
    }

    public Class<?> getOutputFormatClass() {
        return getClass("mapreduce.outputformat.class", Text.class);
    }

    public void setOutputFormatClass(Class<?> outputFormatClass) {
        setClass("mapreduce.outputformat.class", outputFormatClass);
    }

    public String getInputPath() {
        return get("mapreduce.input.path");
    }

    public void setInputPath(String path) {
        set("mapreduce.input.path", path);
    }

    public String getOutputPath() {
        return get("mapreduce.output.path");
    }

    public void setOutputPath(String path) {
        set("mapreduce.output.path", path);
    }
}
