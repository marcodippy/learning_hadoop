package org.mdp.learn.hadoop.order_inversion.pairs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.mdp.learn.hadoop.commons.TextPair;

public class KeyPartitioner extends Partitioner<TextPair, IntWritable> {

  @Override
  public int getPartition(TextPair key, IntWritable value, int numPartitions) {
    return (key.getLeft().hashCode() & Integer.MAX_VALUE) % numPartitions;
  }

}
