package org.mdp.learn.hadoop.cwbtiab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<Text, MapWritable, NullWritable, Text> {
  private Integer TOP;
  private Text    products = new Text();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    TOP = context.getConfiguration().getInt("top_n", 3);
  }

  @Override
  protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
    Map<String, Integer> map = new HashMap<>();
    String product = key.toString();

    for (MapWritable mp : values) {
      mp.forEach((prd, val) -> map.put(prd.toString(), map.getOrDefault(prd.toString(), 0) + ((IntWritable) val).get()));
    }

    emitTopN(context, map, product);
  }

  private void emitTopN(Context context, Map<String, Integer> map, String product) throws IOException, InterruptedException {
    TopNQueue topNQueue = new TopNQueue(TOP);
    map.entrySet().forEach(entry -> topNQueue.addCount(new Count(entry.getKey(), entry.getValue())));
    String alsoBoughtProducts = topNQueue.getTopN().stream().map(cnt -> cnt.toString()).collect(Collectors.joining(","));
    products.set(product + "-" + alsoBoughtProducts);
    context.write(NullWritable.get(), products);
  }

}
