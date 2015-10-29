package org.mdp.learn.hadoop.relational_joins.reduce_side.one_to_one;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class JoinKey implements WritableComparable<JoinKey> {
  private Text        value;
  private IntWritable originTable;

  public JoinKey() {
    this.value = new Text();
    this.originTable = new IntWritable();
  }

  public JoinKey(String value, int originTable) {
    set(value, originTable);
  }

  public void set(String value, int originTable) {
    this.value = new Text(value);
    this.originTable = new IntWritable(originTable);
  }

  public void set(JoinKey otherKey) {
    this.value = new Text(otherKey.value);
    this.originTable = new IntWritable(otherKey.originTable.get());
  }
  
  public Text getValue() {
    return value;
  }

  public IntWritable getOriginTable() {
    return originTable;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    value.write(out);
    originTable.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value.readFields(in);
    originTable.readFields(in);
  }

  @Override
  public int compareTo(JoinKey o) {
    int cmp = value.compareTo(o.value);
    return (cmp != 0) ? cmp : originTable.compareTo(o.originTable);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((originTable == null) ? 0 : originTable.hashCode());
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof JoinKey) {
      JoinKey o = (JoinKey) obj;
      return value.equals(o.value) && originTable.equals(o.originTable);
    }
    return false;
  }

  @Override
  public String toString() {
    return "(" + value + ", " + originTable + ")";
  }

}
