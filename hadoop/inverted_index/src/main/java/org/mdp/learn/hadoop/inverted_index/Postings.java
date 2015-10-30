package org.mdp.learn.hadoop.inverted_index;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.hadoop.io.ArrayWritable;

public class Postings extends ArrayWritable {

  public Postings() {
    super(Posting.class);
  }

  public Postings(Posting[] postings) {
    super(Posting.class);
    set(postings);
  }

  @Override
  public String toString() {
    return super.get() != null ? Arrays.asList(super.toStrings()).stream().collect(Collectors.joining(", ")) : "[]";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Postings) {
      Postings o = (Postings) obj;
      return Arrays.equals(this.get(), o.get());
    }
    return false;
  }

}
