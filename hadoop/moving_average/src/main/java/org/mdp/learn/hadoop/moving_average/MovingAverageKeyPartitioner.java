package org.mdp.learn.hadoop.moving_average;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MovingAverageKeyPartitioner extends Partitioner<MovingAverageKey, FloatWritable> {

  @Override
  public int getPartition(MovingAverageKey key, FloatWritable value, int numPartitions) {
    return (naturalKeyHashCode(key) & Integer.MAX_VALUE) % numPartitions;
  }

  private int naturalKeyHashCode(MovingAverageKey key) {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key.getArrivalAirport() == null) ? 0 : key.getArrivalAirport().hashCode());
    result = prime * result + ((key.getDepartureAirport() == null) ? 0 : key.getDepartureAirport().hashCode());
    return result;
  }

}
