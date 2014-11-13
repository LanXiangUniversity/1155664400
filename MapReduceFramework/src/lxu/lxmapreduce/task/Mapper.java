package lxu.lxmapreduce.task;

import lxu.lxmapreduce.io.RecordReader;
import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.tmp.Configuration;
import lxu.lxmapreduce.tmp.MapContext;
import lxu.lxmapreduce.tmp.TaskID;

import java.io.IOException;

/**
 * Created by Wei on 11/11/14.
 */
public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	// Users should override this function.
	protected void map(KEYIN key, VALUEIN value, Context context) throws IOException {

		context.write((KEYOUT) key, (VALUEOUT) value);
	}

	public void run(Context context) throws IOException {
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
	}

	public class Context extends MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
		public Context(Configuration conf,
		               TaskID taskId,
		               RecordWriter<KEYOUT, VALUEOUT> out,
		               RecordReader<KEYIN, VALUEIN> reader) {
			super(conf, taskId, out, reader);
		}
	}

}
