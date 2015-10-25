package org.mdp.learn.hadoop.moving_average;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MovingAverageKeyComparator extends WritableComparator {

  protected MovingAverageKeyComparator() {
    super(MovingAverageKey.class, true);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    MovingAverageKey mak1 = (MovingAverageKey) a;
    MovingAverageKey mak2 = (MovingAverageKey) b;

    int cmp = mak1.getDepartureAirport().compareTo(mak2.getDepartureAirport());

    if (cmp != 0) return cmp;

    cmp = mak1.getArrivalAirport().compareTo(mak2.getArrivalAirport());

    return (cmp != 0) ? cmp : (mak1.getTimestamp().compareTo(mak2.getTimestamp()));
  }
}
