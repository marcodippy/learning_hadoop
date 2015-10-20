package org.mdp.learn.hadoop.word_count;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private final static IntWritable ONE = new IntWritable(1);
  private final Text word = new Text();

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer tokenizer = new StringTokenizer(value.toString());
    
    while (tokenizer.hasMoreTokens()) {
      String token = tokenizer.nextToken().replaceAll("[, . ; :]", "").toLowerCase();
      word.set(token);
      context.write(word, ONE);
    }
  }
}
