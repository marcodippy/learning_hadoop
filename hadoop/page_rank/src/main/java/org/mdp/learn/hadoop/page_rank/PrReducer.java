package org.mdp.learn.hadoop.page_rank;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import static org.mdp.learn.hadoop.page_rank.PrConstants.*;

public class PrReducer extends Reducer<Text, Text, NullWritable, Text> {

  private Text   row = new Text();
  private double singlePageProbability;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    singlePageProbability = 1d / context.getConfiguration().getLong("number_of_nodes", 0);
  }

  @Override
  protected void reduce(Text nodeId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    Node node = null;
    Double pageRank = PrConstants.RANDOM_JUMP_FACTOR * singlePageProbability;

    for (Text value : values) {
      String stringValue = value.toString();
      if (isNode(stringValue)) {
        node = NodeTextTranslator.parse(stringValue.toString().replace(NODE_VALUE_IDENTIFIER, ""));
      }
      else {
        pageRank += Double.parseDouble(stringValue);
      }
    }

    updateCounters(context, node, pageRank);

    node.setPageRank(pageRank);

    row.set(node.toString());
    context.write(NullWritable.get(), row);
  }

  private void updateCounters(Context context, Node node, Double sumPageRanks) {
    updateCounterIfPageRankChanges(context, node, sumPageRanks);
  }

  private void updateCounterIfPageRankChanges(Context context, Node node, Double newPageRank) {
    int pr = (int) (node.getPageRank() * PrConstants.PRECISION);
    int newPr = (int) (newPageRank * PrConstants.PRECISION);

    if (pr != newPr) {
      context.getCounter(PrCounters.CHANGED_PAGE_RANKS).increment(1);
    }
  }

  private boolean isNode(String val) {
    return val.startsWith(NODE_VALUE_IDENTIFIER);
  }
}
