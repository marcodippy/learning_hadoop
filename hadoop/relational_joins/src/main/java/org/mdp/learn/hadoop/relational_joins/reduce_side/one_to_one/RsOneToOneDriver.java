package org.mdp.learn.hadoop.relational_joins.reduce_side.one_to_one;

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

public class RsOneToOneDriver extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "Reduce Side Join - one to one");

    job.setMapperClass(RsOneToOneMapper.class);
    job.setGroupingComparatorClass(JoinKeyGroupingComparator.class);
    job.setPartitionerClass(JoinKeyPartitioner.class);
    job.setReducerClass(RsOneToOneReducer.class);

    job.setMapOutputKeyClass(JoinKey.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.getConfiguration().set("fieldSeparator", ";");

    int fileNumber = 0;
    
    for (int i = 0; i < args.length - 2; i += 2) {
      String[] parts = args[i].split("/");
      String fileName = parts[parts.length - 1];

      job.getConfiguration().set(fileName + ".joinOrder", Integer.toString(i));
      job.getConfiguration().set(fileName + ".joinKeyIndex", args[i + 1]);
      FileInputFormat.addInputPaths(job, args[i]);
      fileNumber++;      
    }

    job.getConfiguration().set("tableNumber", Integer.toString(fileNumber));
    
    FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

    HdfsUtils.deleteIfExists(getConf(), new Path(args[args.length - 1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new RsOneToOneDriver(), args));
  }
}
