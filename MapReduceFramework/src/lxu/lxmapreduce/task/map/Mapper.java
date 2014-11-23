package lxu.lxmapreduce.task.map;

import lxu.lxmapreduce.io.MapReader;
import lxu.lxmapreduce.io.MapWriter;
import lxu.lxmapreduce.io.RecordReader;
import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.io.format.LongWritable;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.task.TaskAttemptID;
import lxu.lxmapreduce.configuration.Configuration;
import lxu.lxmapreduce.task.TaskID;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by Wei on 11/11/14.
 */
public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	public static void main(String[] args) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		//RecordWriter<Text, Text> output = new LineRecordWriter();
        RecordWriter<Text, Text> output = new MapWriter();
		RecordReader<LongWritable, Text> input = new MapReader();

		input.initialize(null, null);
		output.initialize(null);

		Mapper.Context mapperContext = null;
		Constructor<?>[] contextConstructor = Mapper.Context.class.getConstructors();
//		Constructor<Mapper.Context> contextConstructor = Mapper.Context.class.getConstructor
//				(new Class[]{Mapper.class,
//						Configuration.class,
//						TaskID.class,
//						RecordReader.class,
//						RecordWriter.class});
		System.out.println(contextConstructor[0]);

		Mapper mapper = new Mapper();

		mapperContext = (Mapper.Context) contextConstructor[0].newInstance(
				mapper, new Configuration(), new TaskID("211das", false, 0), output, input);


		// Set input file and output file.
		input.initialize(null, null);
		output.initialize(null);

		mapper.run(mapperContext);
		input.close();
		output.close();
	}

	// Users should override this function.
	protected void map(LongWritable key, Text value, Context context) throws IOException {

		context.write(new Text(key.getValue() + ""), (Text) value);
	}

	public void run(Context context) throws IOException {
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
	}

	public class Context extends MapContext {
		public Context(Configuration conf,
		               TaskAttemptID taskId,
		               RecordWriter<Text, Text> out,
		               RecordReader<LongWritable, Text> reader) {
			super(conf, taskId, out, reader);
		}
	}

}
