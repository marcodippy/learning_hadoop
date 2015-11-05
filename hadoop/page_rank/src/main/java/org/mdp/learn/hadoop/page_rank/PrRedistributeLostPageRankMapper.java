package org.mdp.learn.hadoop.page_rank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mdp.learn.hadoop.page_rank.PrConstants.PrCounters;

public class PrRedistributeLostPageRankMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
  private double lostPageRankMass;
  private long   numberOfNodes;
  private double singlePageProbability;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    lostPageRankMass = (double) Long.parseLong(context.getConfiguration().get("lost_page_rank_mass", "0")) / PrConstants.PRECISION;
    numberOfNodes = Long.parseLong(context.getConfiguration().get("number_of_nodes", "-1"));
    singlePageProbability = 1d / numberOfNodes;
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Node node = NodeTextTranslator.parse(value.toString());
    Double newPageRank = calculatePageRank(node.getPageRank());
    
    updateCounterIfPageRankChanges(context, node, newPageRank);
    
    node.setPageRank(newPageRank);
    value.set(node.toString());
    context.write(NullWritable.get(), value);
  }

  private double calculatePageRank(Double pageRank) {
    return PrConstants.RANDOM_JUMP_FACTOR * singlePageProbability + PrConstants.DAMPING_FACTOR * (lostPageRankMass / numberOfNodes + pageRank);
  }

  private void updateCounterIfPageRankChanges(Context context, Node node, Double newPageRank) {
    int pr = (int) (node.getPageRank() * PrConstants.PRECISION);
    int newPr = (int) (newPageRank * PrConstants.PRECISION);

    if (pr != newPr) {
      context.getCounter(PrCounters.CHANGED_PAGE_RANKS).increment(1);
    }
  }
}
