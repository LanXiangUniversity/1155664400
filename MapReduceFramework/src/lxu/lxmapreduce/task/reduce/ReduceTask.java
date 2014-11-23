package lxu.lxmapreduce.task.reduce;

import lxu.lxdfs.client.ClientOutputStream;
import lxu.lxdfs.metadata.LocatedBlock;
import lxu.lxdfs.service.INameSystemService;
import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.io.format.InputFormat;
import lxu.lxmapreduce.io.format.OutputFormat;
import lxu.lxmapreduce.io.ReduceReader;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.task.Task;
import lxu.lxmapreduce.task.TaskAttemptID;
import lxu.lxmapreduce.task.TaskStatus;
import lxu.lxmapreduce.configuration.Configuration;
import lxu.lxmapreduce.configuration.JobConf;
import lxu.lxmapreduce.task.TaskAttemptContext;
import lxu.utils.ReflectionUtils;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.Socket;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

/**
 * Created by Wei on 11/13/14.
 */
public class ReduceTask extends Task implements Serializable {
    HashSet<String> mapperLocations;
	private INameSystemService nameSystemService;
	public ReduceTask(TaskAttemptID attemptID,
                      int partition,
                      LocatedBlock locatedBlock,
                      HashSet<String> mapperLocations) throws RemoteException, NotBoundException {
		super(attemptID, partition, locatedBlock);
        this.mapperLocations = mapperLocations;
		Configuration conf = new Configuration();
		String masterAddr = conf.getSocketAddr("master.address", "localhost");
		int rmiPort = conf.getInt("rmi.port", 1099);
		Registry registry = LocateRegistry.getRegistry(masterAddr, rmiPort);
		this.nameSystemService = (INameSystemService) registry.lookup("NameSystemService");
	}

    @Override
    public boolean isMapTask() {
        return false;
    }

    @Override
    public void initialize() {
    }

    @Override
	public void run(JobConf jobConf) throws IOException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException, IllegalAccessException, NotBoundException {
        this.runReducer(jobConf);
	}

	public void runReducer(JobConf jobConf) throws ClassNotFoundException, InstantiationException,
			IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException, NotBoundException {
        System.out.println("reduce running");
		int port = 19001;
        System.out.println(mapperLocations.toString());
		HashMap<Text, LinkedList<Text>> reduceInput = new HashMap<>();

		// Init output file path
		List<String> reduceOutput = new ArrayList<String>();
        File reduceOutputFolder = new File("reduceoutput");
        if (!reduceOutputFolder.exists()) {
            reduceOutputFolder.mkdir();
        }
        reduceOutput.add("reduceoutput/part-" + partition);

		for (String addr : mapperLocations) {
			HashMap<Text, LinkedList<Text>> reduceInput1 = null;
			// is map localhost?
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
		putReduceOutput(reduceOutput);
        System.out.println("Reduce finished");
	}

	private void putReduceOutput(List<String> reduceOutput) throws IOException, NotBoundException {
		for (String fileName : reduceOutput) {

			String localFileName = fileName;
			String dfsFileName = this.taskAttemptID.getTaskID() + "-" + fileName;

			List<String> content = new LinkedList<>();

			BufferedReader reader = new BufferedReader(new FileReader(localFileName));
			String line = null;
			while ((line = reader.readLine()) != null) {
				content.add(line);
			}

			String masterAddr = conf.getSocketAddr("master.address", "localhost");
			int rmiPort = conf.getInt("rmi.port", 1099);
			ClientOutputStream cos = new ClientOutputStream(masterAddr, rmiPort);
			cos.setFileName(dfsFileName);
			cos.write(content);
		}
	}


	private HashMap<Text, LinkedList<Text>> getReduceInput(TaskAttemptID taskID) {
		File folder = new File("mapoutput/");
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
