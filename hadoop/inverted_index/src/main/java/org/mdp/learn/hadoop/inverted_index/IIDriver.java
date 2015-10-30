package org.mdp.learn.hadoop.inverted_index;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;
import org.mdp.learn.hadoop.commons.JobBuilder;

public class IIDriver extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

    job.setMapperClass(IIMapper.class);
    job.setPartitionerClass(SecondarySort.TermPartitioner.class);
    job.setGroupingComparatorClass(SecondarySort.TermGroupingComparator.class);
    job.setReducerClass(IIReducer.class);

    job.setMapOutputKeyClass(TermInfo.class);
    job.setMapOutputValueClass(Posting.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Postings.class);

    HdfsUtils.deleteIfExists(getConf(), new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new IIDriver(), args));
  }

}