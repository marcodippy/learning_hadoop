package org.mdp.learn.hadoop.page_rank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PrRedistributeLostPageRankMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
  private long lostPageRankMass;
  private long numberOfNodes;
  private double pageRank;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    lostPageRankMass = Long.parseLong(context.getConfiguration().get("lost_page_rank_mass", "0"));
    numberOfNodes = Long.parseLong(context.getConfiguration().get("number_of_nodes", "-1"));
    
    if (numberOfNodes != -1 && lostPageRankMass > 0) {
      pageRank = ((double)lostPageRankMass / PrConstants.PRECISION) / numberOfNodes;
    }
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Node node = NodeTextTranslator.parse(value.toString());
    node.setPageRank(node.getPageRank() + pageRank);
    value.set(node.toString());
    context.write(NullWritable.get(), value);
  }

}
