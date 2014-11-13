package lxu.lxmapreduce.io;

import lxu.lxmapreduce.io.format.LongWritable;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.tmp.TaskAttemptContext;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Treats keys as offset in file and value as line.
 * Created by Wei on 11/11/14.
 */
public class LineRecordReader extends RecordReader<LongWritable, Text> {
	private int lineNum = 0;
	private LineReader in;
	private LongWritable key = null;
	private Text value = null;

	public LineRecordReader() {
		/* TODO Init LineReader */
		if (this.key == null) {
			//this.key = new LongWritable();
		}

		if (this.value == null) {
			//this.value = new Text();
		}
	}

	@Override
	public void initialize(TaskAttemptContext taskContext) throws FileNotFoundException {
		/* TODO get filename from taskContext */
		this.in = new LineReader("filename");
	}

	@Override
	public Text getCurrentValue() {
		return this.value;
	}

	@Override
	public LongWritable getCurrentKey() {
		return this.key;
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		int res = 0;

		this.key.set(this.lineNum++);
		res = in.readLine(this.value);

		return (res != 0);
	}

	@Override
	public void close() throws IOException {
		if (this.in != null) {
			in.close();
		}
	}
}
