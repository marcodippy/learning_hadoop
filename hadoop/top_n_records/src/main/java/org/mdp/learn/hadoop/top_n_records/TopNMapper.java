package org.mdp.learn.hadoop.top_n_records;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Booking> {

  private TopNQueue topN;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    int N = Integer.parseInt(context.getConfiguration().get("top.n", "10"));
    topN = new TopNQueue(N);
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] values = value.toString().split(";");
    Booking booking = new Booking(values[0], values[1], Integer.parseInt(values[2]));
    topN.addBooking(booking);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    for (Iterator<Booking> iterator = topN.getTopN().iterator(); iterator.hasNext();) {
      context.write(NullWritable.get(), iterator.next());
    }
  }

}
