package lxu.lxmapreduce.task.map;

import lxu.lxmapreduce.configuration.Configuration;
import lxu.lxmapreduce.io.RecordReader;
import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.io.format.LongWritable;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.task.TaskAttemptID;

import java.io.IOException;

/**
 * Created by Wei on 11/11/14.
 */
public class Mapper {

    // Users should override this function.
    protected void map(LongWritable key, Text value, Context context) throws IOException {

        context.write(new Text(key.getValue() + ""), (Text) value);
    }

    public void run(Context context) throws IOException {
        while (context.nextKeyValue()) {
            map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
    }

    public class Context extends MapContext {
        public Context(Configuration conf,
                       TaskAttemptID taskId,
                       RecordWriter<Text, Text> out,
                       RecordReader<LongWritable, Text> reader) {
            super(conf, taskId, out, reader);
        }
    }

}
