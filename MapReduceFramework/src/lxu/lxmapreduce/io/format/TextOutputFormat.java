package lxu.lxmapreduce.io.format;

import lxu.lxmapreduce.io.LineRecordWriter;
import lxu.lxmapreduce.io.RecordWriter;

import java.io.File;

/**
 * Created by magl on 14/11/17.
 */
public class TextOutputFormat extends OutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> createRecordWriter() {
	    return new LineRecordWriter();
    }

	public ReduceWriter createReduceWriter() {
		return new ReduceWriter();
	}
}