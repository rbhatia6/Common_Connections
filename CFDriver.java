import org.apache.log4j.Logger;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;


public class CFDriver extends Configured implements Tool {

    private static final Logger theLogger = Logger.getLogger(CFDriver.class);

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.queuename", "root.mr.intuit.adhoc");

        Job job = new Job(conf);

        job.setJarByClass(CFDriver.class);
        job.setJobName("CFDriver");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);			// mapper will generate key as Text (the keys are as (person1,person2))
        job.setOutputValueClass(Text.class);		// mapper will generate value as Text (list of friends)    

        job.setMapperClass(CFMapper.class);
        job.setReducerClass(CFReducer.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);
        theLogger.info("run(): status=" + status);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: Argument 1: input dir, Argument 2: output dir");
        }

        theLogger.info("inputDir=" + args[0]);
        theLogger.info("outputDir=" + args[1]);
        int jobStatus = submitJob(args);
        theLogger.info("jobStatus=" + jobStatus);
        System.exit(jobStatus);
    }

    public static int submitJob(String[] args) throws Exception {
        int jobStatus = ToolRunner.run(new CFDriver(), args);
        return jobStatus;
    }
}
