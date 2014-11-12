package lxu.lxmapreduce.io;

import java.io.IOException;

/**
 * Created by Wei on 11/11/14.
 */
public class LineRecordWriter<K, V> extends RecordWriter<K, V> {
	private LineWriter out;

	public LineRecordWriter(String s) {

	}

	@Override
	public void initialize() {
		/* TODO Init LineWrite */
	}

	@Override
	public void write(K key, V value) throws IOException {
		this.out.write(key.toString() + " " + value.toString() + "\n");
	}

	@Override
	public void close() {
		this.out.close();
	}

}
