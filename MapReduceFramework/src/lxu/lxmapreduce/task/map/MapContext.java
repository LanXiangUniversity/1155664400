package lxu.lxmapreduce.task.map;

import lxu.lxmapreduce.io.RecordReader;
import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.task.TaskAttemptID;
import lxu.lxmapreduce.configuration.Configuration;
import lxu.lxmapreduce.task.TaskInputOutputContext;

import java.io.IOException;

/**
 * The context that is given to the Mapper.
 * Created by Wei on 11/11/14.
 */
public class MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
		extends TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	private RecordReader<KEYIN, VALUEIN> reader;

	public MapContext(Configuration conf,
	                  TaskAttemptID taskId,
	                  RecordWriter<KEYOUT, VALUEOUT> out,
	                  RecordReader<KEYIN, VALUEIN> reader
	) {
		super(conf, taskId, out);
		this.reader = reader;
	}

	@Override
	public KEYIN getCurrentKey() {
		return this.reader.getCurrentKey();
	}

	@Override
	public VALUEIN getCurrentValue() {
		return this.reader.getCurrentValue();
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		return this.reader.nextKeyValue();
	}
}
