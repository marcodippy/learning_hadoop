package org.mdp.learn.hadoop.cwbtiab;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GroupProductsMapper extends Mapper<LongWritable, Text, Text, Text> {
  private Text userId  = new Text();
  private Text product = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split(",");
    userId.set(fields[0]);
    product.set(fields[1]);
    context.write(userId, product);
  }

}
