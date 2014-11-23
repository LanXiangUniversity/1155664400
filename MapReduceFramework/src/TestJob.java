import lxu.lxmapreduce.io.format.*;
import lxu.lxmapreduce.job.Job;
import lxu.lxmapreduce.task.map.Mapper;
import lxu.lxmapreduce.task.reduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by magl on 14/11/17.
 */
public class TestJob {
    static public class TestMap extends Mapper {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                context.write(new Text(word), new Text("1"));
            }
        }
    }

    static public class TestReduce extends Reducer {
        @Override
        protected void reduce(Text key, Iterator<Text> values, Context context) throws IOException {
            int count = 0;
            while (values.hasNext()) {
                String value = values.next().toString();
                count += Integer.parseInt(value);
            }
            context.write(NullWritable.get(), new Text(key.toString() + "\t" + count));
        }
    }

    public static void main(String[] args) {
		Job job = new Job(new lxu.lxmapreduce.configuration.JobConf(new lxu.lxmapreduce.configuration.Configuration()));
	    job.setJarName("/Users/parasitew/Documents/testDir/Test.jar");
		job.setMapperClass(TestMap.class);
		job.setReducerClass(TestReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputPath("hello");
		job.waitForCompletion();
    }
}
