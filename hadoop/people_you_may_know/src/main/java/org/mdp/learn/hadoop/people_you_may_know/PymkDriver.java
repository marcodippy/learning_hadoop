package org.mdp.learn.hadoop.people_you_may_know;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;
import org.mdp.learn.hadoop.commons.JobBuilder;
import org.mdp.learn.hadoop.commons.TextPair;

public class PymkDriver extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

    job.setMapperClass(PymkMapper.class);
    job.setReducerClass(PymkReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TextPair.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    HdfsUtils.deleteIfExists(getConf(), new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new PymkDriver(), args));
  }
}
