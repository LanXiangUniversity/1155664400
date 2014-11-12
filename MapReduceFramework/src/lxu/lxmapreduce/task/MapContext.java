package lxu.lxmapreduce.task;

import lxu.lxmapreduce.io.RecordReader;

import java.io.IOException;

/**
 * The context that is given to the Mapper.
 * Created by Wei on 11/11/14.
 */
public class MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	private RecordReader<KEYIN, VALUEIN> reader;

	public void MapContext(Configuration conf,
	                       RecordReader<KEYIN, KEYOUT> reade
	                       ) {
		this.reader = reader;
	}


	public void write(KEYOUT key, VALUEOUT value) {

	}

	public KEYIN getCurrentKey() {
		return this.reader.getCurrentKey();
	}

	public VALUEIN getCurrentValue() {
		return this.reader.getCurrentValue();
	}

	public boolean nextKeyValue() throws IOException {
		return reader.nextKeyValue();
	}
}
