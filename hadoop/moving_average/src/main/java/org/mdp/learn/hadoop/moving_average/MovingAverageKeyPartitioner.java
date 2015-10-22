package org.mdp.learn.hadoop.moving_average;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MovingAverageKeyPartitioner extends Partitioner<MovingAverageKey, FloatWritable>{

  @Override
  public int getPartition(MovingAverageKey key, FloatWritable value, int numPartitions) {
    return 0;
  }

}
