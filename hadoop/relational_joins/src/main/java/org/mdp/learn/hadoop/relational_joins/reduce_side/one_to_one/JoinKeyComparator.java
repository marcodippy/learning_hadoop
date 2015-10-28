package org.mdp.learn.hadoop.relational_joins.reduce_side.one_to_one;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinKeyComparator extends WritableComparator {
  protected JoinKeyComparator() {
    super(JoinKey.class, true);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    JoinKey jk1 = (JoinKey) a;
    JoinKey jk2 = (JoinKey) b;

    int cmp = jk1.getValue().compareTo(jk2.getValue());
    return (cmp != 0) ? cmp : (jk1.getOriginTable().compareTo(jk2.getOriginTable()));
  }

}
