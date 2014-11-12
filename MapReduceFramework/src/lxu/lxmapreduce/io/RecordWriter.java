package lxu.lxmapreduce.io;

import java.io.IOException;

/**
 * Write output <key, value> pairs to an output file.
 * Created by Wei on 11/11/14.
 */
public abstract class RecordWriter<K, V> {
	public abstract void initialize();

	public abstract void write(K key, V value) throws IOException;

	public abstract void close();
}
