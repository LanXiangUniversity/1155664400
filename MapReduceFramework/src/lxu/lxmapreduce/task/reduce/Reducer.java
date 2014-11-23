package lxu.lxmapreduce.task.reduce;

import lxu.lxmapreduce.configuration.Configuration;
import lxu.lxmapreduce.io.RecordWriter;
import lxu.lxmapreduce.io.format.NullWritable;
import lxu.lxmapreduce.io.ReduceReader;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.task.TaskAttemptID;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Wei on 11/15/14.
 */
public class Reducer {
    protected void reduce(Text key, Iterator<Text> values, Context context) throws IOException {
        while (values.hasNext()) {
            context.write(NullWritable.get(), new Text(key.toString() + "\t" + values.next().toString()));
        }
    }

    public void run(Context context) throws IOException {

        while (context.nextKeyValue()) {
            reduce(context.getCurrentKey(), context.getCurrentValue(), context);
        }
    }

    public class Context extends ReduceContext {

        public Context(Configuration conf, TaskAttemptID taskId, ReduceReader in, RecordWriter out) {
            super(conf, taskId, in, out);
        }
    }
}
