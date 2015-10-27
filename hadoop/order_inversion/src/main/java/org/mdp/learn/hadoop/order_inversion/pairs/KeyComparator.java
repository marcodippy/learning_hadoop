package org.mdp.learn.hadoop.order_inversion.pairs;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.mdp.learn.hadoop.commons.TextPair;

public class KeyComparator extends WritableComparator {

  protected KeyComparator() {
    super(TextPair.class, true);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable a, WritableComparable b) {
    TextPair t1 = (TextPair) a;
    TextPair t2 = (TextPair) b;

    int cmp = t1.getLeft().compareTo(t2.getLeft());

    if (cmp == 0) {
      if (t1.getRight().toString().equals("*")) {
        cmp = -1;
      }
      else if (t2.getRight().toString().equals("*")) {
        cmp = 1;
      }
      else {
        cmp = t1.getRight().compareTo(t2.getRight());
      }
    }

    return cmp;
  }

}