package lxu.lxmapreduce.task;

import lxu.lxmapreduce.io.LineRecordReader;
import lxu.lxmapreduce.io.LineRecordWriter;
import lxu.lxmapreduce.io.RecordReader;
import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.tmp.Configuration;
import lxu.utils.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by Wei on 11/12/14.
 */
public class MapTask extends Task {

	@Override
	public void run() throws IOException {
		// start thread that will handle communication with parent
//		TaskReporter reporter = new TaskReporter();
//		reporter.startCommunicationThread();
//
//		runNewMapper();
//		done();
	}

	private <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	void runNewMapper() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		// Make task context.

		// Make a mapper
		//Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper =
		//		(Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) ReflectionUtils.loadClassFromJar();


		LineRecordReader<KEYIN, VALUEIN> reader = new LineRecordReader<KEYIN, VALUEIN>();
		LineRecordWriter<KEYOUT, VALUEOUT> writer = new LineRecordWriter<KEYOUT, VALUEOUT>("fasf");
		Configuration conf = new Configuration();


		Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context mapperContext = null;
		Constructor<Mapper.Context> contextConstructor = Mapper.Context.class.getConstructor
				(new Class[]{Mapper.class,
						Configuration.class,
						RecordReader.class,
						RecordWriter.class});

		RecordReader<KEYIN, VALUEIN> input = null;
		RecordWriter<KEYIN, VALUEIN> output = null;

		//mapperContext = contextConstructor.newInstance(mapper, input, output);
		// Create MapperContext.
		// Construct Mapper.Context

		input.initialize();
		//mapper.run(mapperContext);

	}
}