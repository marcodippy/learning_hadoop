package org.mdp.learn.hadoop.moving_average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MovingAverageKey implements WritableComparable<MovingAverageKey> {

  @Override
  public void write(DataOutput out) throws IOException {
    
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    
  }

  @Override
  public int compareTo(MovingAverageKey o) {
    return 0;
  }

}
