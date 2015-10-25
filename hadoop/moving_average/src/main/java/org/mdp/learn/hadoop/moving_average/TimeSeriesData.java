package org.mdp.learn.hadoop.moving_average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TimeSeriesData implements WritableComparable<TimeSeriesData> {
  private long   timestamp;
  private double price;

  public TimeSeriesData() {

  }

  public TimeSeriesData(long timestamp, double price) {
    this.timestamp = timestamp;
    this.price = price;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public double getPrice() {
    return price;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(timestamp);
    out.writeDouble(price);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.timestamp = in.readLong();
    this.price = in.readDouble();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    long temp;
    temp = Double.doubleToLongBits(price);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TimeSeriesData) {
      TimeSeriesData tsa = (TimeSeriesData) obj;
      return this.timestamp == tsa.timestamp && this.price == tsa.price;
    }
    return false;
  }

  @Override
  public int compareTo(TimeSeriesData o) {
    return Long.compare(timestamp, o.timestamp);
  }

  @Override
  public String toString() {
    return "TimeSeriesData [timestamp=" + timestamp + ", price=" + price + "]";
  }

}
