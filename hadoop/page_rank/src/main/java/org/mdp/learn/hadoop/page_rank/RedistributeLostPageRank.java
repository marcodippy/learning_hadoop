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

public class RedistributeLostPageRank {
  private String inputFile;
  private String outputFile;
  private Long   numberOfNodes;
  private Job    job;
  private Long   lostPageRankMass;

  public RedistributeLostPageRank(String inputFile, String outputFile, Long numberOfNodes, Long lostPageRankMass) throws IOException {
    this.inputFile = inputFile;
    this.outputFile = outputFile;
    this.numberOfNodes = numberOfNodes;
    this.lostPageRankMass = lostPageRankMass;
    this.job = createJob(new Configuration());
  }

  private Job createJob(Configuration conf) throws IOException {
    HdfsUtils.deleteIfExists(conf, new Path(outputFile));

    Job job = Job.getInstance(conf, "Redistribute Lost Page Rank Job");
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

  public RedistributeLostPageRank run() throws ClassNotFoundException, IOException, InterruptedException {
    boolean successful = job.waitForCompletion(true);
    if (!successful) {
      throw new RuntimeException("RedistributeLostPageRankMass failed!");
    }
    return this;
  }

  public Long getChangedPageRankNumber() throws IOException {
    return job.getCounters().findCounter(PrCounters.CHANGED_PAGE_RANKS).getValue();
  }

}
