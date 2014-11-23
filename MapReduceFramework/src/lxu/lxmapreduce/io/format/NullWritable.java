package lxu.lxmapreduce.io.format;

/**
 * NullWritable.java
 * Created by Wei on 11/19/14.
 *
 * A wrapper for null.
 */
public class NullWritable {
    public static NullWritable get() {
        return new NullWritable();
    }
}