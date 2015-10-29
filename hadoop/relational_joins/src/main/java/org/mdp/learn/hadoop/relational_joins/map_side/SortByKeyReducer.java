package org.mdp.learn.hadoop.relational_joins.map_side;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortByKeyReducer extends Reducer<Text, Text, NullWritable, Text> {
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    for (Text value : values)
      context.write(NullWritable.get(), value);
  }
}