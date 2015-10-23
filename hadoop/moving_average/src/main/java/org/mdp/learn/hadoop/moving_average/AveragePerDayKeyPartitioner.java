package org.mdp.learn.hadoop.moving_average;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class AveragePerDayKeyPartitioner extends Partitioner<MovingAverageKey, FloatWritable> {

  @Override
  public int getPartition(MovingAverageKey key, FloatWritable value, int numPartitions) {
    return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
  }

}
