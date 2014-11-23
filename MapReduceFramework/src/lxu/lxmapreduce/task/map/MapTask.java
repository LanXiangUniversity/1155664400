package lxu.lxmapreduce.task.map;

import lxu.lxdfs.metadata.LocatedBlock;
import lxu.lxmapreduce.configuration.Configuration;
import lxu.lxmapreduce.configuration.JobConf;
import lxu.lxmapreduce.io.RecordReader;
import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.io.format.InputFormat;
import lxu.lxmapreduce.io.format.OutputFormat;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.task.Task;
import lxu.lxmapreduce.task.TaskAttemptContext;
import lxu.lxmapreduce.task.TaskAttemptID;
import lxu.utils.ReflectionUtils;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Wei on 11/12/14.
 */
public class MapTask extends Task implements Serializable {
    private List<String> inputsplits = new LinkedList<>();
    private List<String> outputFiles = new LinkedList<>();
    private List<LocatedBlock> locatedBlocks = new LinkedList<>();

    public MapTask(TaskAttemptID attemptID, int partition, LocatedBlock locatedBlock) {
        super(attemptID, partition, locatedBlock);

        // TODO: Init inputsplits
        this.locatedBlocks.add(locatedBlock);
        inputsplits.add("blk_" + locatedBlock.getBlock().getBlockID());
    }

    public static void main(String[] args) {

    }

    @Override
    public boolean isMapTask() {
        return true;
    }

    @Override
    public void initialize() {
        int numReduceTasks = conf.getNumReduceTasks();
        String jobID = taskAttemptID.getJobID();
        for (int reduceID = 0; reduceID < numReduceTasks; reduceID++) {
            outputFiles.add(jobID + "_r-" + reduceID + "_" + this.partition);
        }
    }

    @Override
    public void run(JobConf jobConf) throws IOException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        // start thread that will handle communication with parent
//		TaskReporter reporter = new TaskReporter();
//		reporter.startCommunicationThread();
//
        this.runMapper(jobConf);
//		done();
    }

    private
    void runMapper(JobConf jobConf) throws IOException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException,
            InstantiationException, ClassNotFoundException {

        // Create taskContext.
        TaskAttemptContext taskContext = new TaskAttemptContext(jobConf, this.getTaskAttemptID());

        // Create InputFormat (Lin).
        InputFormat inputFormat = (InputFormat)
                ReflectionUtils.newInstance(taskContext.getInputFormatClass());

        OutputFormat outputFormat = (OutputFormat)
                ReflectionUtils.newInstance(taskContext.getOutputFormatClass());

        // Create mapper
        Mapper mapper = (Mapper) ReflectionUtils.newInstance(jobConf.getMapperClass());
        /*
        Mapper<LongWritable, Text, Text, Text> mapper = (Mapper<LongWritable, Text, Text,
                Text>) ReflectionUtils.newInstance(jobConf.getMapperClass());
                */
        /* TODO Init form input block files. */
        // Create LineRecordReader.
        RecordReader input = inputFormat.createRecordReader();
        RecordWriter output = outputFormat.createRecordWriter();

        // Create mapperContext
        Mapper.Context mapperContext = null;
        Constructor<Mapper.Context> contextConstructor = Mapper.Context.class.getConstructor
                (new Class[]{Mapper.class,
                        Configuration.class,
                        TaskAttemptID.class,
                        RecordWriter.class,
                        RecordReader.class});

        // Set input file and output file.
        input.initialize(this.inputsplits, this.locatedBlocks);
        output.initialize(this.outputFiles);

        mapperContext = contextConstructor.newInstance(mapper, jobConf, taskAttemptID, output, input);

        mapper.run(mapperContext);

        input.close();
        output.close();
    }
}