package org.mdp.learn.hadoop.order_inversion.pairs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mdp.learn.hadoop.commons.TextPair;

public class CoOccurrenceMatrixMapperWithPairs extends Mapper<LongWritable, Text, TextPair, IntWritable> {
  private final IntWritable ONE        = new IntWritable(1);
  private final TextPair    pair       = new TextPair();
  private int               neighbours = 2;

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] words = value.toString().replaceAll("--", "").toLowerCase().split("[\\s,;.:?!]+");
    int cnt = 0;

    for (int i = 0; i < words.length; i++) {
      String word = words[i];

      for (int k = i - neighbours; k < i + neighbours; k++) {
        if (k == i || k < 0 || k > words.length - 1) continue;
        context.write(pair.set(word, words[k]), ONE);
        cnt++;
      }

      context.write(pair.set(word, "*"), new IntWritable(cnt));
    }
  }
}