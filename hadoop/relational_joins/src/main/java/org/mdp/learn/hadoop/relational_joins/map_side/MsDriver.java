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
    String tableLeft = args[0];
    String tableRight = args[1];
    String outputFile = args[2];

    String tableLeftSorted = tableLeft + "_sorted";
    String tableRightSorted = tableRight + "_sorted";

    HdfsUtils.deleteIfExists(getConf(), new Path(tableLeftSorted));
    HdfsUtils.deleteIfExists(getConf(), new Path(tableRightSorted));

    sortAndPartition(tableLeft, tableLeftSorted);
    sortAndPartition(tableRight, tableRightSorted);

    Job job = Job.getInstance(getConf(), "Map Side Join");
    job.setJarByClass(MsDriver.class);

    job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", SEPARATOR);
    String joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, tableLeftSorted, tableRightSorted);
    job.getConfiguration().set(CompositeInputFormat.JOIN_EXPR, joinExpression);
    job.getConfiguration().set("separator", SEPARATOR);

    job.setMapperClass(MsMapper.class);
    job.setReducerClass(Reducer.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(CompositeInputFormat.class);

    FileInputFormat.addInputPaths(job, tableLeft + "," + tableRight);
    FileOutputFormat.setOutputPath(job, new Path(outputFile));

    HdfsUtils.deleteIfExists(getConf(), new Path(outputFile));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  private void sortAndPartition(String inputFile, String outputFile) throws Exception {
    Job job = Job.getInstance(getConf(), "Map Side Join - sort " + inputFile);
    job.setJarByClass(MsDriver.class);
    
    job.setMapperClass(SortByKeyMapper.class);
    job.setReducerClass(SortByKeyReducer.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.getConfiguration().set("separator", SEPARATOR);
    
    FileInputFormat.addInputPath(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));

    if (!job.waitForCompletion(true)) System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new MsDriver(), args));
  }
}
