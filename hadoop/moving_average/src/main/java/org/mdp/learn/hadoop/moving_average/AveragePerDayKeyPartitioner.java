package org.mdp.learn.hadoop.moving_average;

import org.apache.hadoop.mapreduce.Partitioner;

public class AveragePerDayKeyPartitioner extends Partitioner<MovingAverageKey, TimeSeriesData> {

  @Override
  public int getPartition(MovingAverageKey key, TimeSeriesData value, int numPartitions) {
    return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
  }

}
