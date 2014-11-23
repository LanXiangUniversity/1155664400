package lxu.lxmapreduce.io.format;

/**
 * Created by Wei on 11/11/14.
 */
public class LongWritable {
    private int value;

    public int get() {
        return value;
    }

    public void set(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public int hashCode() {
        return (value + "").hashCode();
    }
}
