import lxu.lxmapreduce.io.format.*;
import lxu.lxmapreduce.job.Job;
import lxu.lxmapreduce.task.map.Mapper;
import lxu.lxmapreduce.task.reduce.Reducer;
import lxu.lxmapreduce.tmp.Configuration;
import lxu.lxmapreduce.tmp.JobConf;

import java.io.IOException;

/**
 * Created by magl on 14/11/17.
 */
public class TestJob {
    static public class TestMap extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException {
            context.write(key, new Text(value.toString() + " hello"));
        }
    }

    public static void main(String[] args) {
        Job job = new Job(new JobConf(new Configuration()));

        job.setMapperClass(TestMap.class);
        job.setReducerClass(Reducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputPath("hello");

        job.waitForCompletion();
    }
}
