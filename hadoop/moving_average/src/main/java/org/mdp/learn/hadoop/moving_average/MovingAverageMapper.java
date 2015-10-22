package org.mdp.learn.hadoop.moving_average;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovingAverageMapper extends Mapper<LongWritable, Text, MovingAverageKey, FloatWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    
  }

}
