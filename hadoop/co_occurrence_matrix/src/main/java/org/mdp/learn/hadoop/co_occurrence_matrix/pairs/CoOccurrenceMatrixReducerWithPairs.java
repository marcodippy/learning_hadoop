package org.mdp.learn.hadoop.co_occurrence_matrix.pairs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.mdp.learn.hadoop.commons.TextPair;

public class CoOccurrenceMatrixReducerWithPairs extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {
  private final IntWritable sum = new IntWritable();

  @Override
  protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int cnt = 0;

    for (IntWritable val : values)
      cnt += val.get();

    sum.set(cnt);
    context.write(key, sum);
  }

}