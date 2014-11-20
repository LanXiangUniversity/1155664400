package lxu.lxmapreduce.io;

import java.util.LinkedList;
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
	private LinkedList<LineWriter> out;

	public LineRecordWriter() {
	}

	@Override
	public void initialize(List<String> outputFiles) throws FileNotFoundException {
        this.out = new LinkedList<LineWriter>();
        for (String outputFile : outputFiles) {
            out.add(new LineWriter(outputFile));
        }
	}

	@Override
	//public void write(Text key, Text value) throws IOException {
    public void write(LongWritable key, Text value) throws IOException {
        int index = key.hashCode() % out.size();
		this.out.get(index).write(key.getValue() + "\t" + value.getValue() + "\n");
	}

	@Override
	public void close() {
        for (LineWriter o : out) {
            o.close();
        }
	}

}
