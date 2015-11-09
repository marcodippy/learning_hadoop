package org.mdp.learn.hadoop.frequently_bought_together;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FBTMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private Text                     pair = new Text();
  private static final IntWritable ONE  = new IntWritable(1);

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split("-");
    List<String> products = Arrays.asList(fields[1].split(","));
    products.sort(Comparator.naturalOrder());

    Set<String> combinations = new HashSet<>();

    for (String prd1 : products) {
      for (String prd2 : products) {
        if (!prd1.equals(prd2)) {
          combinations.add(buildKey(prd1, prd2));
        }
      }
    }

    for (String pairOfProduct : combinations) {
      pair.set(pairOfProduct);
      context.write(pair, ONE);
    }
  }

  private String buildKey(String prd1, String prd2) {
    return prd1.compareTo(prd2) < 0 ? prd1 + "," + prd2 : prd2 + "," + prd1;
  }

}
