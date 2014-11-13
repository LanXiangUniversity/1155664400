package lxu.lxmapreduce.io;

import lxu.lxmapreduce.io.format.LongWritable;
import lxu.lxmapreduce.io.format.Text;

import java.io.IOException;

/**
 * Treats keys as offset in file and value as line.
 * Created by Wei on 11/11/14.
 */
public class LineRecordReader<K, V> extends RecordReader<K, V> {
	private int lineNum = 0;
	private LineReader in;
	private K key = null;
	private V value = null;

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
	public void initialize() {

	}

	@Override
	public V getCurrentValue() {
		return null;//this.value;
	}

	@Override
	public K getCurrentKey() {
		return null;//this.key;
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		int res = 0;

		//this.key.set(this.lineNum++);
		//res = in.readLine(this.value);

		return (res != 0);
	}

	@Override
	public void close() throws IOException {
		if (this.in != null) {
			in.close();
		}
	}
}
