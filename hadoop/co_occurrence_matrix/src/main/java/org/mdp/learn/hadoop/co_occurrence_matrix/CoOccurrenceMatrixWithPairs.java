package org.mdp.learn.hadoop.co_occurrence_matrix;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;
import org.mdp.learn.hadoop.commons.JobBuilder;
import org.mdp.learn.hadoop.commons.TextPair;

public class CoOccurrenceMatrixWithPairs extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

    job.setMapperClass(CoOccurrenceMatrixWithPairsMapper.class);
    job.setReducerClass(CoOccurrenceMatrixWithPairsReducer.class);

    job.setOutputKeyClass(TextPair.class);
    job.setOutputValueClass(IntWritable.class);

    HdfsUtils.deleteIfExists(getConf(), new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new CoOccurrenceMatrixWithPairs(), args));
  }

  public class CoOccurrenceMatrixWithPairsMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
    private final IntWritable ONE        = new IntWritable(1);
    private final TextPair    pair       = new TextPair();
    private int               neighbours = 2;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] words = value.toString().replaceAll("--", "").toLowerCase().split("[\\s,;.:?!]+");

      for (int i = 0; i < words.length; i++)
        for (int k = i - neighbours; k < i + neighbours; i++) {
          if (k == i || k < 0 || k > words.length - 1) continue;
          context.write(pair.set(words[i], words[k]), ONE);
        }
    }
  }

  public class CoOccurrenceMatrixWithPairsReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {
    private IntWritable sum = new IntWritable();

    @Override
    protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int cnt = 0;

      for (IntWritable val : values)
        cnt += val.get();

      sum.set(cnt);
      context.write(key, sum);
    }

  }
}
