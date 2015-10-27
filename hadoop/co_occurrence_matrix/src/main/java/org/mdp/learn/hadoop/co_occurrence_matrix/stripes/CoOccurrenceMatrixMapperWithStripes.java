package org.mdp.learn.hadoop.co_occurrence_matrix.stripes;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CoOccurrenceMatrixMapperWithStripes extends Mapper<LongWritable, Text, Text, MapWritable> {
  private final Text        word       = new Text();
  private final IntWritable ZERO       = new IntWritable(0);
  private int               neighbours = 2;

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] words = value.toString().replaceAll("--", "").toLowerCase().split("[\\s,;.:?!]+");

    for (int i = 0; i < words.length; i++) {
      MapWritable map = new MapWritable();

      for (int k = i - neighbours; k <= i + neighbours; k++) {
        if (k == i || k < 0 || k > words.length - 1) continue;
        incrementCount(map, new Text(words[k]));
      }

      word.set(words[i]);
      context.write(word, map);
    }
  }

  private void incrementCount(MapWritable map, Text key) {
    IntWritable count = (IntWritable) map.getOrDefault(key, ZERO);
    count.set(count.get() + 1);
    map.put(key, count);
  }
}