package org.mdp.learn.hadoop.dijkstra;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DijkstraReducer extends Reducer<Text, Text, NullWritable, Text> {

  private Text row = new Text();

  @Override
  protected void reduce(Text nodeId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    Integer minDistance = Integer.MAX_VALUE;
    String shortest = "";
    Node node = null;

    for (Text val : values) {
      if (isNode(val)) {
        String stringNode = val.toString().replace("VALUE=", "");
        node = Node.parseNode(stringNode);
        if (node.getDistanceFromSource() < minDistance) {
          minDistance = node.getDistanceFromSource();
          shortest = node.getShortestPath();
        }
      }
      else {
        Integer dst = getDistance(val);
        String path = val.toString().contains("-") ? val.toString().split("-")[1] : "";
        if (dst < minDistance) {
          minDistance = dst;
          shortest = path;
        }
      }
    }

    if (!shortest.equals(node.getShortestPath())) {
      node.setShortestPath(shortest);
      context.getCounter(DijkstraCounters.CHANGED_NODES).increment(1);
    }
    else {

    }

    node.setDistanceFromSource(minDistance);

    row.set(node.toString());

    context.write(NullWritable.get(), row);
  }

  private Integer getDistance(Text val) {
    String dst = val.toString().split("-")[0];
    return "INF".equals(dst) ? Integer.MAX_VALUE : Integer.parseInt(dst);
  }

  private boolean isNode(Text val) {
    return val.toString().startsWith("VALUE=");
  }

}
