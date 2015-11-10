package org.mdp.learn.hadoop.frequently_bought_together;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

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

    List<String> combinations = new ArrayList<>();

    for (int i = 0; i < products.size(); i++) {
      for (int j = i+1; j < products.size(); j++) {
        combinations.add(products.get(i)+","+products.get(j));
      }
    }

    for (String pairOfProduct : combinations) {
      pair.set(pairOfProduct);
      context.write(pair, ONE);
    }
  }

}
