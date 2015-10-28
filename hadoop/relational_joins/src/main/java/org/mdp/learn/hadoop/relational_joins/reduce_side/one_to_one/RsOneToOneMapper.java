package org.mdp.learn.hadoop.relational_joins.reduce_side.one_to_one;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class RsOneToOneMapper extends Mapper<LongWritable, Text, JoinKey, Text> {
  private int     JOIN_KEY_INDEX;
  private int     JOIN_ORDER;
  private String  SEPARATOR;
  private JoinKey joinKey = new JoinKey();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) context.getInputSplit();
    String fileName = fileSplit.getPath().getName();
    SEPARATOR = context.getConfiguration().get("fieldSeparator", ";");
    JOIN_KEY_INDEX = context.getConfiguration().getInt(fileName + ".joinKeyIndex", 0);
    JOIN_ORDER = context.getConfiguration().getInt(fileName + ".joinOrder", 0);
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split(SEPARATOR);
    joinKey.set(fields[JOIN_KEY_INDEX], JOIN_ORDER);
    context.write(joinKey, value);
  }

}
