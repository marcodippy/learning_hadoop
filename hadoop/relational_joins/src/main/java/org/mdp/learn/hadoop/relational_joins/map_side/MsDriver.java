package org.mdp.learn.hadoop.relational_joins.map_side;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;

public class MsDriver extends Configured implements Tool {
  private static final String SEPARATOR = ";";

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "Map Side Join");
    job.setJarByClass(MsDriver.class);

    String tableLeft = args[0];
    String tableRight = args[1];
    String outputFile = args[2];

    job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", SEPARATOR);
    String joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, tableLeft, tableRight);
    job.getConfiguration().set("mapred.join.expr", joinExpression);
    job.getConfiguration().set("separator", SEPARATOR);

    job.setMapperClass(MsMapper.class);
    job.setReducerClass(Reducer.class);
    job.setNumReduceTasks(0);

    job.setMapOutputKeyClass(Text.class);
    job.setInputFormatClass(CompositeInputFormat.class);

    FileInputFormat.addInputPaths(job, tableLeft + "," + tableRight);
    FileOutputFormat.setOutputPath(job, new Path(outputFile));

    HdfsUtils.deleteIfExists(getConf(), new Path(outputFile));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new MsDriver(), args));
  }
}
