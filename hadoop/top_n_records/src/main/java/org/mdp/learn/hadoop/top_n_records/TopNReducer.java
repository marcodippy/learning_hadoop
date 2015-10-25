package org.mdp.learn.hadoop.top_n_records;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<NullWritable, Booking, Text, IntWritable> {
  private TopNQueue topN;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    int N = Integer.parseInt(context.getConfiguration().get("top.n", "10"));
    topN = new TopNQueue(N);
  }

  @Override
  protected void reduce(NullWritable key, Iterable<Booking> values, Context context)
      throws IOException, InterruptedException {
    values.forEach(booking -> topN.addBooking(booking));
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    Set<Booking> orderedResults = new TreeSet<>((b1, b2) -> -1 * b1.getCount().compareTo(b2.getCount()));
    orderedResults.addAll(topN.getTopN());

    for (Booking booking : orderedResults) {
      context.write(booking.getFlight(), booking.getCount());
    }
  }

}
