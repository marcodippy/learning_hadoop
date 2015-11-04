package org.mdp.learn.hadoop.page_rank;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import static org.mdp.learn.hadoop.page_rank.PrConstants.*;

public class PrReducer extends Reducer<Text, Text, NullWritable, Text> {

  private Text row = new Text();

  @Override
  protected void reduce(Text nodeId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    Node node = null;
    Double sumPageRanks = 0d;

    for (Text value : values) {
      String stringValue = value.toString();
      if (isNode(stringValue)) {
        node = NodeTextTranslator.parse(stringValue.toString().replace(NODE_VALUE_IDENTIFIER, ""));
      }
      else {
        sumPageRanks += Double.parseDouble(stringValue);
      }
    }

    node.setPageRank(sumPageRanks);

    row.set(node.toString());
    context.write(NullWritable.get(), row);
  }

  private boolean isNode(String val) {
    return val.startsWith(NODE_VALUE_IDENTIFIER);
  }
}
