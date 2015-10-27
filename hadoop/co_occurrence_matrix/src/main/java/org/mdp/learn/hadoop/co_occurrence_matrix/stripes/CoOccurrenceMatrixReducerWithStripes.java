package org.mdp.learn.hadoop.co_occurrence_matrix.stripes;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CoOccurrenceMatrixReducerWithStripes extends Reducer<Text, MapWritable, Text, MapWritable> {
  private final IntWritable ZERO = new IntWritable(0);

  @Override
  protected void reduce(Text key, Iterable<MapWritable> stripes, Context context) throws IOException, InterruptedException {
    MapWritable map = new MapWritable();

    for (MapWritable stripe : stripes)
      stripe.forEach((neighbour, count) -> updateCount(map, (Text) neighbour, (IntWritable) count));

    context.write(key, map);
  }

  private void updateCount(MapWritable map, Text key, IntWritable cnt) {
    IntWritable count = (IntWritable) map.getOrDefault(key, ZERO);
    count.set(count.get() + cnt.get());
    map.put(key, count);
  }
}