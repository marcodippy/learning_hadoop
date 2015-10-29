package org.mdp.learn.hadoop.relational_joins.memory_backed;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;

public class MBDriver extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "Memory Baked join");
    job.setJarByClass(MBDriver.class);

    String smallTable = args[0];
    String bigTable = args[1];
    String outputFile = args[2];

    job.addCacheFile(new URI(smallTable + "#cachedSmallTable"));
    job.getConfiguration().set("separator", ";");

    job.setMapperClass(MBMapper.class);
    job.setReducerClass(Reducer.class);
    job.setNumReduceTasks(0);

    FileInputFormat.addInputPaths(job, bigTable);
    FileOutputFormat.setOutputPath(job, new Path(outputFile));

    HdfsUtils.deleteIfExists(getConf(), new Path(outputFile));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new MBDriver(), args));
  }
}
