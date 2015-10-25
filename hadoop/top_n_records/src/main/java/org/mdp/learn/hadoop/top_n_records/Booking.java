package org.mdp.learn.hadoop.top_n_records;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Booking implements WritableComparable<Booking> {
  private final Text flight;
  private final IntWritable count;

  public Booking() {
    flight = new Text();
    count = new IntWritable(0);
  }
  
  public Booking(Booking booking) {
    flight = new Text(booking.getFlight());
    count = new IntWritable(booking.getCount().get());
  }

  public Booking(String depAirport, String arrAirport, int count) {
    this.flight = new Text(depAirport + "-" + arrAirport);
    this.count = new IntWritable(count);
  }

  public Booking(String flight, int count) {
    this.flight = new Text(flight);
    this.count = new IntWritable(count);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    flight.write(out);
    count.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    flight.readFields(in);
    count.readFields(in);
  }

  public Text getFlight() {
    return flight;
  }

  public IntWritable getCount() {
    return count;
  }

  @Override
  public int compareTo(Booking o) {
    int cmp = flight.compareTo(o.flight);
    return (cmp != 0) ? cmp : count.compareTo(o.count);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((count == null) ? 0 : count.hashCode());
    result = prime * result + ((flight == null) ? 0 : flight.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Booking) {
      Booking other = (Booking) obj;
      return flight.equals(other.flight) && count.equals(other.count);
    }
    return false;
  }

  @Override
  public String toString() {
    return "Booking [flight=" + flight + ", count=" + count + "]";
  }

}
