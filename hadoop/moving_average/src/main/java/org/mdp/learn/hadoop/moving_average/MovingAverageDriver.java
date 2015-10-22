package org.mdp.learn.hadoop.moving_average;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;
import org.mdp.learn.hadoop.commons.JobBuilder;

public class MovingAverageDriver extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

    job.setMapperClass(MovingAverageMapper.class);
    job.setPartitionerClass(MovingAverageKeyPartitioner.class);
    job.setSortComparatorClass(MovingAverageKeyComparator.class);
    job.setGroupingComparatorClass(MovingAverageKeyGroupingComparator.class);
    job.setReducerClass(MovingAverageReducer.class);
    
    job.setMapOutputKeyClass(MovingAverageKey.class);
    job.setMapOutputValueClass(FloatWritable.class);

    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");

    HdfsUtils.deleteIfExists(getConf(), new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new MovingAverageDriver(), args));
  }
}
