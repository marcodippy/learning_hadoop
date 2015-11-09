package org.mdp.learn.hadoop.cwbtiab;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
  private final Text               prodId = new Text();
  private final MapWritable        map    = new MapWritable();
  private static final IntWritable ZERO   = new IntWritable(0);

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split("-");
    List<String> products = Arrays.asList(fields[1].split(","));

    for (String prod : products) {
      for (String otherProd : products) {
        if (!otherProd.equals(prod)) {
          incrementCount(otherProd);
        }
      }
      prodId.set(prod);
      context.write(prodId, map);
      map.clear();
    }

  }

  private void incrementCount(String prod) {
    Text tmpKey = new Text(prod);
    IntWritable count = (IntWritable) map.getOrDefault(tmpKey, ZERO);
    map.put(tmpKey, new IntWritable(count.get() + 1));
  }

}
