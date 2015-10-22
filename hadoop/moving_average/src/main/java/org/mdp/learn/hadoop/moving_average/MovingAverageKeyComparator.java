package org.mdp.learn.hadoop.moving_average;

import org.apache.hadoop.io.WritableComparator;

public class MovingAverageKeyComparator extends WritableComparator{

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return super.compare(b1, s1, l1, b2, s2, l2);
  }

}
