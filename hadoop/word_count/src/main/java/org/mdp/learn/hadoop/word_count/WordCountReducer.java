package org.mdp.learn.hadoop.word_count;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  private final IntWritable totalWordCount = new IntWritable();

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int sum = 0;

    for (IntWritable value : values) {
      sum += value.get();
    }

    totalWordCount.set(sum);

    context.write(key, totalWordCount);
  }
}
