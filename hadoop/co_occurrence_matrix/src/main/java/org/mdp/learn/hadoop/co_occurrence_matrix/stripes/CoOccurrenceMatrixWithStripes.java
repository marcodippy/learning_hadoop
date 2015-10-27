package org.mdp.learn.hadoop.co_occurrence_matrix.stripes;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;
import org.mdp.learn.hadoop.commons.JobBuilder;

public class CoOccurrenceMatrixWithStripes extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

    job.setMapperClass(CoOccurrenceMatrixMapperWithStripes.class);
    job.setReducerClass(CoOccurrenceMatrixReducerWithStripes.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritable.class);

    HdfsUtils.deleteIfExists(getConf(), new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new CoOccurrenceMatrixWithStripes(), args));
  }
}
