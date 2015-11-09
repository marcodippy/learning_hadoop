package org.mdp.learn.hadoop.frequently_bought_together;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FBTReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  private IntWritable sum = new IntWritable();

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int cnt = 0;

    for (IntWritable intWritable : values) {
      cnt += intWritable.get();
    }

    sum.set(cnt);
    context.write(key, sum);
  }

}
