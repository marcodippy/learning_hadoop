package org.mdp.learn.hadoop.inverted_index;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.mdp.learn.hadoop.commons.TextPair;

public class IIMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
  private Map<String, Integer> map   = new HashMap<>();
  private TextPair             tuple = new TextPair();
  private IntWritable          count;
  private String               fileName;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    extractTerms(value).forEach(term -> map.put(term, map.getOrDefault(term, 0) + 1));
  }

  private List<String> extractTerms(Text value) {
    return Arrays.asList(value.toString().split(" "));
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    Set<Entry<String, Integer>> entries = map.entrySet();
    for (Entry<String, Integer> entry : entries) {
      tuple.set(entry.getKey(), fileName);
      count.set(entry.getValue());
      context.write(tuple, count);
    }
    // clean resources
  }

}
