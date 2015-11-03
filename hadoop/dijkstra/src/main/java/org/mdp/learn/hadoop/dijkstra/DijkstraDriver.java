package org.mdp.learn.hadoop.dijkstra;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;
import org.mdp.learn.hadoop.commons.JobBuilder;

public class DijkstraDriver extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

    job.setMapperClass(DijkstraMapper.class);
    job.setReducerClass(DijkstraReducer.class);

    HdfsUtils.deleteIfExists(getConf(), new Path(args[1]));

    do {
      if (!job.waitForCompletion(true)) {
        return 1;
      }
    } while (job.getCounters().findCounter(DijkstraCounters.CHANGED_NODES).getValue() != 0);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new DijkstraDriver(), args));
  }
}
