package lxu.lxmapreduce.io.format;

import lxu.lxmapreduce.io.RecordReader;

/**
 * Created by Wei on 11/12/14.
 */
public abstract class InputFormat<K, V> {
	public abstract RecordReader<K, V> createRecordReader();
}
