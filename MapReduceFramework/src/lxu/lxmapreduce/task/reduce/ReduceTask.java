package lxu.lxmapreduce.task.reduce;

import lxu.lxdfs.metadata.LocatedBlock;
import lxu.lxmapreduce.io.RecordReader;
import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.io.format.InputFormat;
import lxu.lxmapreduce.io.format.OutputFormat;
import lxu.lxmapreduce.io.format.ReduceReader;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.task.Task;
import lxu.lxmapreduce.task.TaskAttemptID;
import lxu.lxmapreduce.tmp.Configuration;
import lxu.lxmapreduce.tmp.JobConf;
import lxu.lxmapreduce.tmp.TaskAttemptContext;
import lxu.utils.ReflectionUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;

/**
 * Created by Wei on 11/13/14.
 */
public class ReduceTask extends Task implements Serializable {
	public ReduceTask(TaskAttemptID attemptID, int partition, LocatedBlock locatedBlock) {
		super(attemptID, partition, locatedBlock);
	}

    @Override
    public boolean isMapTask() {
        return false;
    }

    @Override
    public void initialize() {
        // TODO move connection here
    }

    @Override
	public void run(JobConf jobConf) throws IOException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.runReducer(jobConf);
	}

    // TODO move initilization to initialize()
	public void runReducer(JobConf jobConf) throws ClassNotFoundException, InstantiationException,
			IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException {
        System.out.println("reduce running");
		int port = 19001;
		String[] mapperAddrs = jobConf.getSocketAddrs();
		// TODO:Init input file
		Map<Text, Iterator<Text>> reduceInput = new HashMap<>();

		// TODO:Init output file path
		List<String> reduceOutput = new ArrayList<String>();
        reduceOutput.add("part-" + partition);

		for (String addr : mapperAddrs) {
			// TODO: is localhost?
			Socket sock = new Socket(addr, port);
			ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
			out.writeObject(this.getTaskAttemptID());
			System.err.println("R: Ask for input");
			ObjectInputStream in = new ObjectInputStream(sock.getInputStream());
			reduceInput = (Map<Text, Iterator<Text>>) in.readObject();
			System.err.println("R: Get input");

			in.close();
			out.close();
			sock.close();
		}

		taskContext = new TaskAttemptContext(jobConf, this.getTaskAttemptID());

		Reducer reducer =
				(Reducer) ReflectionUtils.newInstance(jobConf.getReducerClass());

		InputFormat inputFormat = (InputFormat)
				ReflectionUtils.newInstance(taskContext.getInputFormatClass());

		OutputFormat outputFormat = (OutputFormat)
				ReflectionUtils.newInstance(taskContext.getOutputFormatClass());

		ReduceReader input = new ReduceReader();
		RecordWriter output = outputFormat.createRecordWriter();

		Reducer.Context reducerContext = null;
		Constructor<Reducer.Context> contextConstructor = Reducer.Context.class.getConstructor
				(new Class[]{Reducer.class,
						Configuration.class,
                        TaskAttemptID.class,
						//RecordReader.class,
                        ReduceReader.class,
						RecordWriter.class});

		// Set input file and output file.
		input.initialize(reduceInput);
		output.initialize(reduceOutput);

		reducerContext = contextConstructor.newInstance(reducer, jobConf, taskAttemptID, input, output);

		reducer.run(reducerContext);

		input.close();
		output.close();
        System.out.println("Reduce finished");
	}
}
