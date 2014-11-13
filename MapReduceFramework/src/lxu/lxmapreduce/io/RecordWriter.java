package lxu.lxmapreduce.io;

import lxu.lxmapreduce.tmp.TaskAttemptContext;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Write output <key, value> pairs to an output file.
 * Created by Wei on 11/11/14.
 */
public abstract class RecordWriter<K, V> {
	public abstract void initialize(TaskAttemptContext taskContext) throws FileNotFoundException;

	public abstract void write(K key, V value) throws IOException;

	public abstract void close();
}
