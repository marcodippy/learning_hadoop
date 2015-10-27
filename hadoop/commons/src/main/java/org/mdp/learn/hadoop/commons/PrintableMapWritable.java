package org.mdp.learn.hadoop.commons;

import org.apache.hadoop.io.MapWritable;

public class PrintableMapWritable extends MapWritable {

  @Override
  public String toString() {
    return MyWritableUtils.mapToString(this);
  }

}
