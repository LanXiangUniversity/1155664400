package lxu.lxmapreduce.io.format;

import lxu.lxmapreduce.io.LineRecordReader;
import lxu.lxmapreduce.io.RecordReader;

import java.io.InputStream;

/**
 * Created by Wei on 11/12/14.
 */
public class TextInputFormat extends InputFormat<LongWritable, Text> {
	@Override
	public RecordReader<LongWritable, Text> createRecordReader() {
		return new LineRecordReader();
	}
}
