package lxu.lxmapreduce.io.format;

import lxu.lxmapreduce.io.RecordReader;

/**
 * InputFormat.java
 * Created by Wei on 11/12/14.
 *
 * The input format of a task.
 */
public abstract class InputFormat<K, V> {
    public abstract RecordReader<K, V> createRecordReader();
}
