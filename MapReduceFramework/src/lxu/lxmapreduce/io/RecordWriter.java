package lxu.lxmapreduce.io;

import java.util.List;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * RecordWriter.java
 * Created by Wei on 11/11/14.
 *
 * Write output key, value pairs to an output file.
 */
public abstract class RecordWriter<K, V> {
	public abstract void initialize(List<String> outputFiles) throws FileNotFoundException;

	public abstract void write(K key, V value) throws IOException;

	public abstract void close();
}
