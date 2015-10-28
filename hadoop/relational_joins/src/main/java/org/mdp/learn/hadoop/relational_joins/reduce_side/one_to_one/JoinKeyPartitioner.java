package org.mdp.learn.hadoop.relational_joins.reduce_side.one_to_one;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class JoinKeyPartitioner extends Partitioner<JoinKey, Text> {

  @Override
  public int getPartition(JoinKey key, Text value, int numPartitions) {
    return (key.getValue().hashCode() & Integer.MAX_VALUE) % numPartitions;
  }

}
