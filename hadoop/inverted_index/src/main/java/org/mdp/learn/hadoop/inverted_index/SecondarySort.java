package org.mdp.learn.hadoop.inverted_index;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySort {
  public static class TermPartitioner extends Partitioner<TermInfo, Posting> {
    @Override
    public int getPartition(TermInfo key, Posting value, int numPartitions) {
      return (key.getTerm().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
  }

  public static class TermGroupingComparator extends WritableComparator {
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      TermInfo ti1 = (TermInfo) a;
      TermInfo ti2 = (TermInfo) b;
      return ti1.getTerm().compareTo(ti2.getTerm());
    }

  }
}
