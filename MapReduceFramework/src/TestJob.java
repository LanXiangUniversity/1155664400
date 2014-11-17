import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.io.format.TextInputFormat;
import lxu.lxmapreduce.io.format.TextOutputFormat;
import lxu.lxmapreduce.io.format.TextOutputWriter;
import lxu.lxmapreduce.job.Job;
import lxu.lxmapreduce.task.map.Mapper;
import lxu.lxmapreduce.task.reduce.Reducer;
import lxu.lxmapreduce.tmp.Configuration;
import lxu.lxmapreduce.tmp.JobConf;

/**
 * Created by magl on 14/11/17.
 */
public class TestJob {
    public static void main(String[] args) {
        Job job = new Job(new JobConf(new Configuration()));

        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputPath("hello");

        job.waitForCompletion();
    }
}
