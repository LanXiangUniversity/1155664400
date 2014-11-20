package lxu.lxmapreduce.task.reduce;

import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.io.format.ReduceReader;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.task.TaskAttemptID;
import lxu.lxmapreduce.tmp.Configuration;
import lxu.lxmapreduce.tmp.TaskInputOutputContext;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Wei on 11/12/14.
 */
public class ReduceContext extends TaskInputOutputContext {
	private ReduceReader reader;

	public ReduceContext(Configuration conf, TaskAttemptID taskId, ReduceReader reader, RecordWriter out) {
		super(conf, taskId, out);

		this.reader = reader;
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		return this.reader.nextKeyValue();
	}

	@Override
	public Text getCurrentKey() {
		return this.reader.getCurrentKey();
	}

	@Override
	public Iterator<Text> getCurrentValue() throws IOException {
		return this.reader.getCurrentValue();
	}
}
