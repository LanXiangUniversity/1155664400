import lxu.lxmapreduce.io.format.*;
import lxu.lxmapreduce.job.Job;
import lxu.lxmapreduce.task.map.Mapper;
import lxu.lxmapreduce.task.reduce.Reducer;
import lxu.lxmapreduce.configuration.Configuration;
import lxu.lxmapreduce.configuration.JobConf;

import java.io.IOException;
import java.util.Iterator;

public class PageRankDriver {

    static public class PreprocessMap extends Mapper {
        protected void map(LongWritable key, Text value, Context context) throws IOException {
            String valueString = value.toString();
            if (valueString.startsWith("#")) {
                return;
            }
            String[] nodes = valueString.split("\t");
            context.write(new Text(nodes[0]), new Text(nodes[1]));
            // For nodes with no out edge
            context.write(new Text(nodes[1]), new Text("#"));
        }
    }

    static public class PreprocessReduce extends Reducer {
        protected void reduce(Text key, Iterator<Text> values, Context context) throws IOException {
            StringBuilder outputValue = new StringBuilder();
            outputValue.append(key.toString()).append("\t1");
            String delim = "\t";
            while (values.hasNext()) {
                String outNode = values.next().toString();
                if (!outNode.equals("#")) {
                    outputValue.append(delim).append(outNode);
                    delim = ",";
                }
            }
            context.write(NullWritable.get(), new Text(outputValue.toString()));
        }
    }

    static public class Map extends Mapper {

        protected void map(LongWritable key, Text value, Context context) throws IOException {
            String[] info = value.toString().split("\t");
            String outNodes = "";
            if (info.length == 3) {
                outNodes = "\t" + info[2];
                String[] outs = info[2].split(",");
                for (String out : outs) {
                    // used for calculating importance
                    context.write(new Text(out), new Text("i" + info[1]));
                }
            } else if  (info.length == 2) {
                // do nothing
            } else {
                return;
            }
            // used for next iteration
            context.write(new Text(info[0]), new Text("n" + info[1] + outNodes));
		}
	}

    static public class Reduce extends Reducer {
        protected void reduce(Text key, Iterator<Text> values, Context context) throws IOException {
            int importance = 0;
            String outNodes = "";
            while (values.hasNext()) {
                String valueString = values.next().toString();
                if (valueString.startsWith("i")) {
                    importance += Integer.parseInt(valueString.substring(1));
                } else if (valueString.startsWith("n")) {
                    String[] info = valueString.substring(1).split("\t");
                    importance += Integer.parseInt(info[0]);
                    if (info.length == 2) {
                        outNodes = "\t" + info[1];
                    }
                } else {
                    continue;
                }
            }
            context.write(NullWritable.get(), new Text(key.toString() + "\t" + importance + outNodes));
		}
	}

	public static void main(String[] args) throws Exception {
        Job job = new Job(new JobConf(new Configuration()));

		job.setJobName("PageRank Preprocessing");

		job.setMapperClass(PreprocessMap.class);
		job.setReducerClass(PreprocessReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputPath("pageranktest");

        job.waitForCompletion();

        /*
        for (int i = 0; i < 5; i++) {
            System.out.println("Iteration " + (i + 1));

            conf = new JobConf(PageRankDriver.class);
            conf.setJobName("PageRank Iteration" + (i + 1));

            conf.setMapperClass(Map.class);
            conf.setMapOutputKeyClass(Text.class);
            conf.setMapOutputValueClass(Text.class);

            conf.setReducerClass(Reduce.class);
            conf.setOutputKeyClass(NullWritable.class);
            conf.setOutputValueClass(Text.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(conf, new Path("/tmpoutput/" + args[0] + i));
            if (i == 4) {
                FileOutputFormat.setOutputPath(conf, new Path(args[1]));
            } else {
                FileOutputFormat.setOutputPath(conf, new Path("/tmpoutput/" + args[0] + (i + 1)));
            }
            JobClient.runJob(conf);
        }
        */
	}
}
