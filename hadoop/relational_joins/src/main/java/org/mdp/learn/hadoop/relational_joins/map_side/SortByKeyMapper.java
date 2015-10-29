package org.mdp.learn.hadoop.relational_joins.map_side;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortByKeyMapper extends Mapper<LongWritable, Text, Text, Text> {
  private Text   joinKey = new Text();
  private String SEPARATOR;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    SEPARATOR = context.getConfiguration().get("separator");
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    joinKey.set(value.toString().split(SEPARATOR)[0]);
    context.write(joinKey, value);
  }
}