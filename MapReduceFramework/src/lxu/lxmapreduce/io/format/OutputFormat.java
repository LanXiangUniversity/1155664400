package lxu.lxmapreduce.io.format;

import lxu.lxmapreduce.io.RecordWriter;

/**
 * Created by Wei on 11/12/14.
 */
public abstract class OutputFormat<K, V> {
	public abstract RecordWriter<K, V> createRecordWriter();
}
