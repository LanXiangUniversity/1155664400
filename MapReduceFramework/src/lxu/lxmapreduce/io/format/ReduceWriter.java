package lxu.lxmapreduce.io.format;

import lxu.lxmapreduce.io.LineWriter;
import lxu.lxmapreduce.io.RecordWriter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * Created by Wei on 11/19/14.
 */
public class ReduceWriter extends RecordWriter<NullWritable, Text> {
	private LineWriter out;

	public ReduceWriter() {

	}

	@Override
	public void initialize(List<String> outputFiles) throws FileNotFoundException {
		this.out = new LineWriter(outputFiles.get(0));
	}

	@Override
	//public void write(Null key, Text value) throws IOException {
	public void write(NullWritable key, Text value) throws IOException {
		this.out.write(value.getValue() + "\n");
	}

	@Override
	public void close() {
		this.out.close();
	}

}
