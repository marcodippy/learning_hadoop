package org.mdp.learn.hadoop.cwbtiab;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;

public class CwbtiabDriver extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    String inputFile = args[0], outputFile = args[1], tmpOutputFile = "tmp_cwbtiab_output";

    Job groupProds = getGroupProductsJob(inputFile, tmpOutputFile);

    if (groupProds.waitForCompletion(true)) {
      Job topNJob = getTopNJob(tmpOutputFile, outputFile);
      return topNJob.waitForCompletion(true) ? 0 : 1;
    }

    return 1;
  }

  private Job getGroupProductsJob(String inputFile, String outputFile) throws IllegalArgumentException, IOException {
    Job job = Job.getInstance(getConf(), "Group Products Job");
    job.setJarByClass(CwbtiabDriver.class);
    FileInputFormat.addInputPath(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));

    job.setMapperClass(GroupProductsMapper.class);
    job.setReducerClass(GroupProductsReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    HdfsUtils.deleteIfExists(getConf(), new Path(outputFile));

    return job;
  }

  private Job getTopNJob(String inputFile, String outputFile) throws IllegalArgumentException, IOException {
    Job job = Job.getInstance(getConf(), "Top N Job");
    job.setJarByClass(CwbtiabDriver.class);
    FileInputFormat.addInputPath(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));

    job.setMapperClass(TopNMapper.class);
    job.setReducerClass(TopNReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritable.class);

    HdfsUtils.deleteIfExists(getConf(), new Path(outputFile));

    return job;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new CwbtiabDriver(), args));
  }
}
