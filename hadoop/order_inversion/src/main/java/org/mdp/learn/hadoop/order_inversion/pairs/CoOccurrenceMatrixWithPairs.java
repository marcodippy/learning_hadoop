package org.mdp.learn.hadoop.order_inversion.pairs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;
import org.mdp.learn.hadoop.commons.JobBuilder;
import org.mdp.learn.hadoop.commons.TextPair;

public class CoOccurrenceMatrixWithPairs extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

    job.setMapperClass(CoOccurrenceMatrixMapperWithPairs.class);
    job.setReducerClass(CoOccurrenceMatrixReducerWithPairs.class);

    job.setOutputKeyClass(TextPair.class);
    job.setOutputValueClass(IntWritable.class);

    HdfsUtils.deleteIfExists(getConf(), new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new CoOccurrenceMatrixWithPairs(), args));
  }

}
