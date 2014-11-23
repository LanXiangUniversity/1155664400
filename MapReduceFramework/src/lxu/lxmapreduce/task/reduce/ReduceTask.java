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
import lxu.lxmapreduce.task.TaskStatus;
import lxu.lxmapreduce.tmp.Configuration;
import lxu.lxmapreduce.tmp.JobConf;
import lxu.lxmapreduce.tmp.TaskAttemptContext;
import lxu.utils.ReflectionUtils;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by Wei on 11/13/14.
 */
public class ReduceTask extends Task implements Serializable {
    HashSet<String> mapperLocations;
	public ReduceTask(TaskAttemptID attemptID,
                      int partition,
                      LocatedBlock locatedBlock,
                      HashSet<String> mapperLocations) {
		super(attemptID, partition, locatedBlock);
        this.mapperLocations = mapperLocations;
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
        System.out.println(mapperLocations.toString());
		// TODO:Init input file
		HashMap<Text, LinkedList<Text>> reduceInput = new HashMap<>();

		// TODO:Init output file path
		List<String> reduceOutput = new ArrayList<String>();
        reduceOutput.add("part-" + partition);

		for (String addr : mapperLocations) {
			HashMap<Text, LinkedList<Text>> reduceInput1 = null;
			// TODO: is map localhost?
			if (addr == InetAddress.getLocalHost().getHostAddress()) {
				reduceInput1 = getReduceInput(this.taskAttemptID);
			} else {
				try {
					Socket sock = new Socket(addr, port);
					ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
					out.writeObject(this.getTaskAttemptID());
					System.err.println("R: Ask for input");
					ObjectInputStream in = new ObjectInputStream(sock.getInputStream());
					reduceInput1 = (HashMap<Text, LinkedList<Text>>) in.readObject();
					System.err.println("R: Get input");
					System.err.println("reduceinput size" + reduceInput.size());
					in.close();
					out.close();
					sock.close();
				} catch (Exception e) {
					System.err.println("Reduce Task fails: Cannot connect to this tasktracker.");
					this.taskStatus.setState(TaskStatus.FAILED);
					e.printStackTrace();
					return;
				}
			}

			// Merge reduce input
			for (Text text : reduceInput1.keySet()) {
				LinkedList<Text> values1 = reduceInput1.get(text);
				if (!reduceInput.containsKey(text)) {
					reduceInput.put(text, values1);
				} else {
					LinkedList<Text> values = reduceInput.get(text);
					values.addAll(values1);
				}
			}
		}

		taskContext = new TaskAttemptContext(jobConf, this.getTaskAttemptID());

		Reducer reducer =
				(Reducer) ReflectionUtils.newInstance(jobConf.getReducerClass());

		InputFormat inputFormat = (InputFormat)
				ReflectionUtils.newInstance(taskContext.getInputFormatClass());

		OutputFormat outputFormat = (OutputFormat)
				ReflectionUtils.newInstance(taskContext.getOutputFormatClass());

		ReduceReader input = new ReduceReader();
		RecordWriter output = outputFormat.createReduceWriter();

		Reducer.Context reducerContext = null;
		Constructor<Reducer.Context> contextConstructor = Reducer.Context.class.getConstructor
				(new Class[]{Reducer.class,
						Configuration.class,
                        TaskAttemptID.class,
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


	private HashMap<Text, LinkedList<Text>> getReduceInput(TaskAttemptID taskID) {
		File folder = new File(".");
		String namePrefix = taskID.getTaskID().toString();
		HashMap<Text, LinkedList<Text>> contents = new HashMap<Text, LinkedList<Text>>();
		for (File fileEntry : folder.listFiles()) {
			if (fileEntry.isFile() && fileEntry.getName().startsWith(namePrefix)) {
				try {
					BufferedReader reader = new BufferedReader(new FileReader(fileEntry));
					String line = null;
					while ((line = reader.readLine()) != null) {
						String[] info = line.split("\t");
						Text key = new Text(info[0]);
						Text value = new Text(info[1]);
						LinkedList<Text> values = contents.get(key);
						if (values == null) {
							values = new LinkedList<Text>();
							contents.put(key, values);
						}
						values.add(value);

					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return contents;
	}
}
