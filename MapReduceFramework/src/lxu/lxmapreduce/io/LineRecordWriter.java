package lxu.lxmapreduce.io;

import lxu.lxmapreduce.io.format.LongWritable;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.tmp.TaskAttemptContext;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by Wei on 11/11/14.
 */
public class LineRecordWriter extends RecordWriter<LongWritable, Text> {
	private LineWriter out;

	public LineRecordWriter() {
	}

	@Override
	public void initialize(TaskAttemptContext taskContext) throws FileNotFoundException {
		/* TODO get filename from taskContext */
		this.out = new LineWriter("/Users/parasitew/Documents/testDir/1/out.txt");
	}

	@Override
	public void write(LongWritable key, Text value) throws IOException {
		this.out.write(key.getValue() + " " + value.getValue() + "\n");
	}

	@Override
	public void close() {
		this.out.close();
	}

}
