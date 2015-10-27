package org.mdp.learn.hadoop.order_inversion.stripes;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.mdp.learn.hadoop.commons.PrintableMapWritable;

public class CoOccurrenceMatrixReducerWithStripes extends Reducer<Text, MapWritable, Text, MapWritable> {
  private final IntWritable ZERO = new IntWritable(0);

  @Override
  protected void reduce(Text key, Iterable<MapWritable> stripes, Context context) throws IOException, InterruptedException {
    MapWritable map = new PrintableMapWritable();

    stripes.forEach((stripe) -> stripe.forEach((neighbour, count) -> updateCount(map, neighbour, (IntWritable) count)));

    int marginal = getMarginal(map);

    context.write(key, getFrequencyMap(map, marginal));
  }

  private MapWritable getFrequencyMap(MapWritable map, int marginal) {
    MapWritable frequencies = new PrintableMapWritable();
    map.forEach((neighbor, cnt) -> frequencies.put(neighbor, new DoubleWritable(((IntWritable) cnt).get() / (double) marginal)));
    return frequencies;
  }

  private int getMarginal(MapWritable map) {
    return map.values().stream().mapToInt(v -> ((IntWritable) v).get()).sum();
  }

  private void updateCount(MapWritable map, Writable key, IntWritable cnt) {
    IntWritable count = (IntWritable) map.getOrDefault(key, ZERO);
    map.put(key, new IntWritable(count.get() + cnt.get()));
  }
}