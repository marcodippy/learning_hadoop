package org.mdp.learn.hadoop.inverted_index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Posting implements WritableComparable<Posting>{

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public int compareTo(Posting o) {
    // TODO Auto-generated method stub
    return 0;
  }

}
