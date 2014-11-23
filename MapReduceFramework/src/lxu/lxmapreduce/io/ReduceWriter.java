package lxu.lxmapreduce.io;

import lxu.lxmapreduce.io.format.NullWritable;
import lxu.lxmapreduce.io.format.Text;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * ReduceWriter.java
 * Created by Wei on 11/19/14.
 *
 * This class writes the output of reducer to file.
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
	public void write(NullWritable key, Text value) throws IOException {
		this.out.write(value.getValue() + "\n");
	}

	@Override
	public void close() {
		this.out.close();
	}

}
