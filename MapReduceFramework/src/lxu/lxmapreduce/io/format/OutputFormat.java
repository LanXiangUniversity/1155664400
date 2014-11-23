package lxu.lxmapreduce.io.format;

import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.io.ReduceWriter;

/**
 * OutputFormat.java
 * Created by Wei on 11/12/14.
 *
 * The output format of a task.
 */
public abstract class OutputFormat<K, V> {
    public abstract RecordWriter<K, V> createRecordWriter();

    public abstract ReduceWriter createReduceWriter();
}
