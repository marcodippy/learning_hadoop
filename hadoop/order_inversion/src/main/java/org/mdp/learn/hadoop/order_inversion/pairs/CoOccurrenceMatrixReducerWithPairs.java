package org.mdp.learn.hadoop.order_inversion.pairs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.mdp.learn.hadoop.commons.TextPair;

public class CoOccurrenceMatrixReducerWithPairs extends Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {
  private static final Text STAR     = new Text("*");
  private Integer           marginal = 0;
  private Text              previousWord;

  @Override
  protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    if (key.getRight().equals(STAR)) {
      if (!key.getLeft().equals(previousWord)) marginal = 0;

      for (IntWritable cnt : values)
        marginal += cnt.get();
    }
    else {
      int neighborCount = 0;

      for (IntWritable cnt : values)
        neighborCount += cnt.get();

      context.write(key, new DoubleWritable((double) neighborCount / marginal));
    }

    previousWord = key.getLeft();
  }

}