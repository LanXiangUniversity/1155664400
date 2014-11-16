package lxu.lxmapreduce.task.reduce;

import lxu.lxmapreduce.io.RecordReader;
import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.task.TaskAttemptID;
import lxu.lxmapreduce.tmp.Configuration;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Wei on 11/15/14.
 */
public class Reducer {
	protected void reduce(Text key, Iterator<Text> values, Context context) throws IOException {
		while (values.hasNext()) {
			context.write((Text) key, (Text) values.next());
		}
	}

	public void run(Context context) throws IOException {
		while (context.nextKeyValue()) {
			reduce(context.getCurrentKey(), context.getCurrentValue(), context);
		}
	}

	public abstract class Context extends ReduceContext {

		public Context(Configuration conf, TaskAttemptID taskId, RecordReader in, RecordWriter out) {
			super(conf, taskId, out);
		}
	}
}
