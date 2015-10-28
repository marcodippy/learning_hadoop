package org.mdp.learn.hadoop.relational_joins.reduce_side.one_to_one;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinKeyGroupingComparator extends WritableComparator {
  protected JoinKeyGroupingComparator() {
    super(JoinKey.class, true);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    JoinKey jk1 = (JoinKey) a;
    JoinKey jk2 = (JoinKey) b;

    return jk1.getValue().compareTo(jk2.getValue());
  }

}
