package org.mdp.learn.hadoop.average_grade_revisited;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class Sum implements Writable {
  private final IntWritable sum, count;

  public Sum() {
    this.sum = new IntWritable(0);
    this.count = new IntWritable(0);
  }

  public Sum(IntWritable sum) {
    this.sum = sum;
    this.count = new IntWritable(1);
  }

  public Sum(int sum) {
    this.sum = new IntWritable(sum);
    this.count = new IntWritable(1);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    sum.readFields(in);
    count.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    sum.write(out);
    count.write(out);
  }

  public IntWritable getSum() {
    return sum;
  }

  public IntWritable getCount() {
    return count;
  }

  public void add(int val) {
    count.set(count.get() + 1);
    sum.set(sum.get() + val);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((count == null) ? 0 : count.hashCode());
    result = prime * result + ((sum == null) ? 0 : sum.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Sum) {
      Sum ip = (Sum) obj;
      return count.equals(ip.count) && sum.equals(ip.sum);
    }
    return false;
  }

  @Override
  public String toString() {
    return "Sum [sum=" + sum + ", count=" + count + "]";
  }

}