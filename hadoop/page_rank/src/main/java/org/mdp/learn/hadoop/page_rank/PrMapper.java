package org.mdp.learn.hadoop.page_rank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import static org.mdp.learn.hadoop.page_rank.PrConstants.*;

public class PrMapper extends Mapper<LongWritable, Text, Text, Text> {

  private Text nodeId = new Text();
  private Text val    = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String textValue = value.toString();
    Node node = NodeTextTranslator.parse(textValue);

    emitNodeAsIs(context, textValue, node);

    if (!node.getLinks().isEmpty()) {
      Double pageRankContribution = PrConstants.DAMPING_FACTOR * (node.getPageRank() / node.getLinks().size());

      for (String destNodeId : node.getLinks()) {
        emitLink(context, destNodeId, pageRankContribution);
      }
    }
    else {
      updateLostPageRankMass(node.getPageRank(), context);
    }
  }

  private void emitNodeAsIs(Context context, String textValue, Node node) throws IOException, InterruptedException {
    nodeId.set(node.getId());
    val.set(NODE_VALUE_IDENTIFIER + textValue);
    context.write(nodeId, val);
  }

  private void emitLink(Context context, String destNodeId, Double pageRankContribution) throws IOException, InterruptedException {
    nodeId.set(destNodeId);
    val.set(pageRankContribution.toString());
    context.write(nodeId, val);
  }

  private void updateLostPageRankMass(Double lostPageRank, Context context) {
    int pr = (int) (lostPageRank * PrConstants.PRECISION);
    context.getCounter(PrCounters.LOST_PAGE_RANK_MASS).increment(pr);
  }
}
