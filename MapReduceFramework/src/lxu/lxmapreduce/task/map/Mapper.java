package lxu.lxmapreduce.task.map;

import lxu.lxmapreduce.io.LineRecordReader;
import lxu.lxmapreduce.io.LineRecordWriter;
import lxu.lxmapreduce.io.RecordReader;
import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.io.format.LongWritable;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.task.TaskAttemptID;
import lxu.lxmapreduce.tmp.Configuration;
import lxu.lxmapreduce.tmp.TaskID;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by Wei on 11/11/14.
 */
public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	public static void main(String[] args) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		//RecordWriter<Text, Text> output = new LineRecordWriter();
        RecordWriter<LongWritable, Text> output = new LineRecordWriter();
		RecordReader<LongWritable, Text> input = new LineRecordReader();

		input.initialize(null);
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
		input.initialize(null);
		output.initialize(null);

		mapper.run(mapperContext);
		input.close();
		output.close();
	}

	// Users should override this function.
	protected void map(KEYIN key, VALUEIN value, Context context) throws IOException {

		context.write((KEYOUT) key, (VALUEOUT) value);
	}

	public void run(Context context) throws IOException {
		while (context.nextKeyValue()) {
            System.out.println("context has nextKeyValue");
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
	}

	public class Context extends MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
		public Context(Configuration conf,
		               TaskAttemptID taskId,
		               RecordWriter<KEYOUT, VALUEOUT> out,
		               RecordReader<KEYIN, VALUEIN> reader) {
			super(conf, taskId, out, reader);
		}
	}

}
