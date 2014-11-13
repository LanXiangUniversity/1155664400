package lxu.lxmapreduce.io;

import lxu.lxmapreduce.io.format.LongWritable;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.io.format.TextInputFormat;

import java.io.IOException;

/**
 * Created by Wei on 11/11/14.
 */
public class LineRecordWriter extends RecordWriter<LongWritable, Text> {
	private LineWriter out;

	public LineRecordWriter(String s) {

	}

	@Override
	public void initialize() {
		/* TODO Init LineWrite */
	}

	@Override
	public void write(LongWritable key, Text value) throws IOException {
		this.out.write(key.toString() + " " + value.toString() + "\n");
	}

	@Override
	public void close() {
		this.out.close();
	}

}
