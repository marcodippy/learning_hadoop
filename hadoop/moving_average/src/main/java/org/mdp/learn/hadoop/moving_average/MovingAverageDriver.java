package org.mdp.learn.hadoop.moving_average;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;

public class MovingAverageDriver extends Configured implements Tool {

  private static final String TEMP_OUTPUT_FILE_PATH = "tmp_moving_average_output";

  public int run(String[] args) throws Exception {
    HdfsUtils.deleteIfExists(getConf(), new Path(args[1]));
    HdfsUtils.deleteIfExists(getConf(), new Path(TEMP_OUTPUT_FILE_PATH));

    Job averagePerDayJob = getAveragePerDayJob(args);

    boolean isJobSuccessful = averagePerDayJob.waitForCompletion(true);

    if (isJobSuccessful) {
      Job movingAverageJob = getMovingAverageJob(args);
      isJobSuccessful = movingAverageJob.waitForCompletion(true);
    }

    return isJobSuccessful ? 0 : 1;
  }

  private Job getAveragePerDayJob(String[] args) throws IllegalArgumentException, IOException {
    Job job = Job.getInstance(getConf());
    job.setJarByClass(MovingAverageDriver.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(TEMP_OUTPUT_FILE_PATH));

    job.setMapperClass(MovingAverageMapper.class);
    job.setPartitionerClass(AveragePerDayKeyPartitioner.class);
    job.setSortComparatorClass(MovingAverageKeyComparator.class);
    job.setGroupingComparatorClass(MovingAverageKeyComparator.class);
    job.setReducerClass(AveragePerDayReducer.class);

    job.setMapOutputKeyClass(MovingAverageKey.class);
    job.setMapOutputValueClass(TimeSeriesData.class);

    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");

    job.getConfiguration().set("departureAirport_index", "1");
    job.getConfiguration().set("arrivalAirport_index", "2");
    job.getConfiguration().set("timestamp_index", "3");
    job.getConfiguration().set("price_index", "5");

    return job;
  }

  private Job getMovingAverageJob(String[] args) throws IllegalArgumentException, IOException {
    Job job = Job.getInstance(getConf());
    job.setJarByClass(MovingAverageDriver.class);

    FileInputFormat.addInputPath(job, new Path(TEMP_OUTPUT_FILE_PATH));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(MovingAverageMapper.class);
    job.setPartitionerClass(MovingAverageKeyPartitioner.class);
    job.setSortComparatorClass(MovingAverageKeyComparator.class);
    job.setGroupingComparatorClass(MovingAverageKeyGroupingComparator.class);
    job.setReducerClass(MovingAverageReducer.class);

    job.setMapOutputKeyClass(MovingAverageKey.class);
    job.setMapOutputValueClass(TimeSeriesData.class);

    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
    
    job.getConfiguration().set("WindowSize", "3");

    job.getConfiguration().set("departureAirport_index", "0");
    job.getConfiguration().set("arrivalAirport_index", "1");
    job.getConfiguration().set("timestamp_index", "2");
    job.getConfiguration().set("price_index", "3");

    return job;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new MovingAverageDriver(), args));
  }
}
