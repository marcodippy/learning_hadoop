package org.mdp.learn.hadoop.page_rank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mdp.learn.hadoop.commons.HdfsUtils;
import org.mdp.learn.hadoop.page_rank.PrConstants.PrCounters;

public class PageRankJob {

  private String inputFile;
  private String outputFile;
  private Long   numberOfNodes;
  private Job    job;

  public PageRankJob(String inputFile, String outputFile, Long numberOfNodes) throws IOException {
    this.inputFile = inputFile;
    this.outputFile = outputFile;
    this.numberOfNodes = numberOfNodes;
    this.job = createJob(new Configuration());
  }

  private Job createJob(Configuration conf) throws IOException {
    HdfsUtils.deleteIfExists(conf, new Path(outputFile));

    Job job = Job.getInstance(conf, "Page Rank Job");
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

  public PageRankJob run() throws ClassNotFoundException, IOException, InterruptedException {
    boolean successful = job.waitForCompletion(true);
    if (!successful) {
      throw new RuntimeException("PageRankJob failed!");
    }
    return this;
  }

  public Long getChangedPageRankNumber() throws IOException {
    return job.getCounters().findCounter(PrCounters.CHANGED_PAGE_RANKS).getValue();
  }

  public Long getLostPageRankMass() throws IOException {
    return job.getCounters().findCounter(PrCounters.LOST_PAGE_RANK_MASS).getValue();
  }
  
  public void printStatus () throws IOException {

    System.out.println("**************************** PAGE RANK JOB STATUS ****************************");
    System.out.println("changedPageRanks " + getChangedPageRankNumber());
    System.out.println("lostPageRankMass " + getLostPageRankMass());
    System.out.println("******************************************************************************");

  }
}
