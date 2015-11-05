package org.mdp.learn.hadoop.page_rank;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;
import org.mdp.learn.hadoop.page_rank.PrConstants.PrCounters;

public class PrDriver extends Configured implements Tool {
  public int run(String[] args) throws Exception {

    String inputFile = args[0];
    String outputFile = args[1];

    long changedPageRanks = -1, lostPageRankMass = -1, numberOfNodes = -1;
    int iterations = 0;

    String tmpInputFile = inputFile;
    String tmpOutputFile = outputFile + iterations;
    
    do {
      Job pageRank = getCalculatePageRankJob(tmpInputFile, tmpOutputFile, numberOfNodes);

      if (!pageRank.waitForCompletion(true)) {
        return 1;
      }

      changedPageRanks = pageRank.getCounters().findCounter(PrCounters.CHANGED_PAGE_RANKS).getValue();
      lostPageRankMass = pageRank.getCounters().findCounter(PrCounters.LOST_PAGE_RANK_MASS).getValue();

      System.out.println("************************************************************");
      System.out.println("changedPageRanks " + changedPageRanks);
      System.out.println("lostPageRankMass " + lostPageRankMass);
      System.out.println("iterations " + iterations);
      System.out.println("************************************************************");
      
      if (numberOfNodes == -1) numberOfNodes = pageRank.getCounters().findCounter(PrCounters.NUMBER_OF_NODES).getValue();

//      if (lostPageRankMass > 0) {
//        Job redistributeLostPageRank = getRedistributePageRankJob(outputFile, outputFile+iterations, lostPageRankMass, numberOfNodes);
//
//        if (!redistributeLostPageRank.waitForCompletion(true)) {
//          return 1;
//        }
//      }
      
      iterations++;
      tmpInputFile = tmpOutputFile;
      tmpOutputFile = outputFile + iterations;
    }
    while (changedPageRanks > 0);

    System.out.println("Job finished in " + iterations + " iterations");

    return 0;
  }

  public Job getCalculatePageRankJob(String inputFile, String outputFile, long numberOfNodes) throws IOException {
    HdfsUtils.deleteIfExists(getConf(), new Path(outputFile));

    Job job = Job.getInstance(getConf(), "Page Rank Job");
    job.setJarByClass(PrDriver.class);
    FileInputFormat.addInputPath(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));

    job.setMapperClass(PrMapper.class);
    job.setReducerClass(PrReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    job.getConfiguration().setLong("number_of_nodes", numberOfNodes);

    return job;
  }

  public Job getRedistributePageRankJob(String inputFile, String outputFile, long lostPageRankMass, long numberOfNodes) throws IOException {
    HdfsUtils.deleteIfExists(getConf(), new Path(outputFile));

    Job job = Job.getInstance(getConf(), "Redistribute Page Rank Job");
    job.setJarByClass(PrDriver.class);
    FileInputFormat.addInputPath(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));

    job.setMapperClass(PrRedistributeLostPageRankMapper.class);
    job.setNumReduceTasks(0);

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.getConfiguration().setLong("lost_page_rank_mass", lostPageRankMass);
    job.getConfiguration().setLong("number_of_nodes", numberOfNodes);

    return job;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new PrDriver(), args));
  }
}
