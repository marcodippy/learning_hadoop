package org.mdp.learn.hadoop.moving_average;

import org.apache.hadoop.mapreduce.Partitioner;

public class MovingAverageKeyPartitioner extends Partitioner<MovingAverageKey, TimeSeriesData> {

  @Override
  public int getPartition(MovingAverageKey key, TimeSeriesData value, int numPartitions) {
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
