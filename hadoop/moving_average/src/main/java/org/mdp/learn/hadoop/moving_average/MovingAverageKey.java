package org.mdp.learn.hadoop.moving_average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MovingAverageKey implements WritableComparable<MovingAverageKey> {
  private Text departureAirport, arrivalAirport;
  private LongWritable timestamp;

  public MovingAverageKey() {
    this.departureAirport = new Text();
    this.arrivalAirport = new Text();
    this.timestamp = new LongWritable();
  }

  public MovingAverageKey(String departureAirport, String arrivalAirport, Long timestamp) {
    this.departureAirport = new Text(departureAirport);
    this.arrivalAirport = new Text(arrivalAirport);
    this.timestamp = new LongWritable(timestamp);
  }

  public Text getDepartureAirport() {
    return departureAirport;
  }

  public Text getArrivalAirport() {
    return arrivalAirport;
  }

  public LongWritable getTimestamp() {
    return timestamp;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    departureAirport.write(out);
    arrivalAirport.write(out);
    timestamp.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    departureAirport.readFields(in);
    arrivalAirport.readFields(in);
    timestamp.readFields(in);
  }

  @Override
  public int compareTo(MovingAverageKey o) {
    int cmp = this.departureAirport.compareTo(o.departureAirport);

    if (cmp == 0) {
      cmp = this.arrivalAirport.compareTo(o.arrivalAirport);
      if (cmp == 0)
        cmp = this.timestamp.compareTo(o.timestamp);
    }

    return cmp;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((arrivalAirport == null) ? 0 : arrivalAirport.hashCode());
    result = prime * result + ((departureAirport == null) ? 0 : departureAirport.hashCode());
    result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MovingAverageKey) {
      MovingAverageKey mak = (MovingAverageKey) obj;
      return departureAirport.equals(mak.departureAirport) && arrivalAirport.equals(mak.arrivalAirport)
          && timestamp.equals(mak.timestamp);
    }

    return false;
  }

}
