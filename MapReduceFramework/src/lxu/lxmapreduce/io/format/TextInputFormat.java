package lxu.lxmapreduce.io.format;

import lxu.lxmapreduce.io.MapReader;
import lxu.lxmapreduce.io.RecordReader;

/**
 * TextInputSteam.java
 * Created by Wei on 11/12/14.
 *
 * A abstraction for a file. Key is the line number and value is the
 * corresponding line.
 */
public class TextInputFormat extends InputFormat<LongWritable, Text> {
	@Override
	public RecordReader<LongWritable, Text> createRecordReader() {
		return new MapReader();
	}
}
