package org.mdp.learn.hadoop.cwbtiab;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.mdp.learn.hadoop.commons.PrintableMapWritable;

public class TopNReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
  private MapWritable              map  = new PrintableMapWritable();
  private static final IntWritable ZERO = new IntWritable(0);

  @Override
  protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
    for (MapWritable mp : values) {
      mp.forEach((prd, val) -> incrementCount(prd.toString(), ((IntWritable) val).get()));
    }
    emitTopN(context, key, 5);
  }

  private void emitTopN(Context context, Text key, int TOP) throws IOException, InterruptedException {
    TopNQueue topNQueue = new TopNQueue(TOP);

    for (Entry<Writable, Writable> entry : map.entrySet()) {
      topNQueue.addCount(new Count(entry.getKey().toString(), ((IntWritable) entry.getValue()).get()));
    }

    for (Count cnt : topNQueue.getTopN()) {
      map.put(new Text(cnt.getProduct()), new IntWritable(cnt.getCount()));
    }

    context.write(key, map);
    map.clear();
  }

  private void incrementCount(String prod, Integer incr) {
    Text tmpKey = new Text(prod);
    IntWritable count = (IntWritable) map.getOrDefault(tmpKey, ZERO);
    map.put(tmpKey, new IntWritable(count.get() + incr));
  }
}
