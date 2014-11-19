package lxu.lxmapreduce.io;

import java.util.List;
import lxu.lxmapreduce.io.format.LongWritable;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.tmp.TaskAttemptContext;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by Wei on 11/11/14.
 */
//public class LineRecordWriter extends RecordWriter<Text, Text> {
public class LineRecordWriter extends RecordWriter<LongWritable, Text> {
	private LineWriter out;

	public LineRecordWriter() {
	}

	@Override
	public void initialize(List<String> outputFiles) throws FileNotFoundException {
		this.out = new LineWriter(outputFiles.get(0));
	}

	@Override
	//public void write(Text key, Text value) throws IOException {
    public void write(LongWritable key, Text value) throws IOException {
        System.out.println(key.getValue() + " " + value.getValue());
		this.out.write(key.getValue() + " " + value.getValue() + "\n");
	}

	@Override
	public void close() {
		this.out.close();
	}

}
