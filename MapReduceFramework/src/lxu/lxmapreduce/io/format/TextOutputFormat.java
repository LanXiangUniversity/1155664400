package lxu.lxmapreduce.io.format;

import lxu.lxmapreduce.io.MapWriter;
import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.io.ReduceWriter;

/**
 * TextOutputFormat.java
 * Created by magl on 14/11/17.
 *
 * A abstraction for Text Output. Key and value are seperated by "\t".
 */
public class TextOutputFormat extends OutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> createRecordWriter() {
	    return new MapWriter();
    }

	public ReduceWriter createReduceWriter() {
		return new ReduceWriter();
	}
}