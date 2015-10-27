package org.mdp.learn.hadoop.order_inversion.pairs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.mdp.learn.hadoop.commons.TextPair;

public class CoOccurrenceMatrixReducerWithPairs extends Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {
  private static final Text STAR        = new Text("*");
  private DoubleWritable    marginal    = new DoubleWritable();
  private Text              currentWord = new Text();

  @Override
  protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    if (key.getRight().equals(STAR)) {
      if (key.getLeft().equals(currentWord)) {
        marginal.set(marginal.get() + sumValues(values));
      }
      else {
        currentWord.set(key.getLeft());
        marginal.set(sumValues(values));
      }
    }
    else {
      int neighborCount = sumValues(values);
      context.write(key, new DoubleWritable((double) neighborCount / marginal.get()));
    }
  }

  private int sumValues(Iterable<IntWritable> values) {
    int count = 0;

    for (IntWritable value : values)
      count += value.get();
    
    return count;
  }

}