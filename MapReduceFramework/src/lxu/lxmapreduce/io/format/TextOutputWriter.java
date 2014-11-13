package lxu.lxmapreduce.io.format;

import lxu.lxmapreduce.io.LineRecordWriter;
import lxu.lxmapreduce.io.RecordWriter;

/**
 * Created by Wei on 11/12/14.
 */
public class TextOutputWriter extends OutputFormat<LongWritable, Text> {
	@Override
	public RecordWriter<LongWritable, Text> createRecordWriter() {
		return new LineRecordWriter();
	}
}
