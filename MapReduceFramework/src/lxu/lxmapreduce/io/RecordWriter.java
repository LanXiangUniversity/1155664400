package lxu.lxmapreduce.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * Write output <key, value> pairs to an output file.
 * Created by Wei on 11/11/14.
 */
public abstract class RecordWriter<K, V> {
    public abstract void initialize(List<String> outputFiles) throws FileNotFoundException;

    public abstract void write(K key, V value) throws IOException;

    public abstract void close();
}
