package lxu.lxmapreduce.task.map;

import lxu.lxmapreduce.configuration.Configuration;
import lxu.lxmapreduce.io.RecordReader;
import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.io.format.LongWritable;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.task.TaskAttemptID;
import lxu.lxmapreduce.task.TaskInputOutputContext;

import java.io.IOException;

/**
 * The context that is given to the Mapper.
 * Created by Wei on 11/11/14.
 */
public class MapContext
        extends TaskInputOutputContext<LongWritable, Text, Text, Text> {
    private RecordReader<LongWritable, Text> reader;

    public MapContext(Configuration conf,
                      TaskAttemptID taskId,
                      RecordWriter<Text, Text> out,
                      RecordReader<LongWritable, Text> reader
    ) {
        super(conf, taskId, out);
        this.reader = reader;
    }

    @Override
    public LongWritable getCurrentKey() {
        return this.reader.getCurrentKey();
    }

    @Override
    public Text getCurrentValue() {
        return this.reader.getCurrentValue();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        return this.reader.nextKeyValue();
    }
}
