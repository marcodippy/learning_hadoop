package org.mdp.learn.hadoop.commons;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {
  private Text left, right;

  public TextPair() {
    left = new Text();
    right = new Text();
  }

  public TextPair(String left, String right) {
    set(left, right);
  }

  public TextPair set(String left, String right) {
    this.left = new Text(left);
    this.right = new Text(right);
    return this;
  }

  public Text getLeft() {
    return left;
  }

  public Text getRight() {
    return right;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    left.write(out);
    right.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    left.readFields(in);
    right.readFields(in);
  }

  @Override
  public int compareTo(TextPair o) {
    int cmp = left.compareTo(o.left);
    if (cmp == 0) cmp = right.compareTo(o.right);
    return cmp;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((left == null) ? 0 : left.hashCode());
    result = prime * result + ((right == null) ? 0 : right.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TextPair) {
      TextPair o = (TextPair) obj;
      return left.equals(o.left) && right.equals(o.right);
    }
    return false;
  }

  @Override
  public String toString() {
    return "(" + left + ", " + right + ")";
  }

}
