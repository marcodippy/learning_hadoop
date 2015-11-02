package org.mdp.learn.hadoop.graph_bfs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BfsReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

  private Text row = new Text();

  @Override
  protected void reduce(LongWritable nodeId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    Integer minDistance = Integer.MAX_VALUE;

    Node node = null;

    for (Text val : values) {
      if (isNode(val)) {
        String stringNode = val.toString().replace("VALUE=", "");
        node = Node.parseNode(stringNode);
        minDistance = Math.min(node.getDistanceFromSource(), minDistance);     
      }
      else {
        Integer dst = getDistance(val);
        minDistance = Math.min(dst, minDistance);
      }
    }

    node.setDistanceFromSource(minDistance);

    row.set(node.toString());
    context.write(NullWritable.get(), row);
  }

  private Integer getDistance(Text val) {
    String dst = val.toString();
    return "INF".equals(dst) ? Integer.MAX_VALUE : Integer.parseInt(dst);
  }

  private boolean isNode(Text val) {
    return val.toString().startsWith("VALUE=");
  }

}
