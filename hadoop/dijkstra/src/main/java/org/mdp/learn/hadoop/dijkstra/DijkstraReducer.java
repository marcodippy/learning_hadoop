package org.mdp.learn.hadoop.dijkstra;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import static org.mdp.learn.hadoop.dijkstra.DijkstraConstants.*;

public class DijkstraReducer extends Reducer<Text, Text, NullWritable, Text> {

  private Text row = new Text();

  @Override
  protected void reduce(Text nodeId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    Integer minDistance = INFINITE_VALUE;
    String shortestPath = "";
    Node node = null;

    for (Text value : values) {
      String stringValue = value.toString();
      if (isNode(stringValue)) {
        node = NodeTextTranslator.parse(stringValue.toString().replace(NODE_VALUE_IDENTIFIER, ""));

        if (node.getDistanceFromSource() < minDistance) {
          minDistance = node.getDistanceFromSource();
          shortestPath = node.getShortestPath();
        }
      }
      else { //EDGE = (DISTANCE-PATH)
        Integer dst = getDistance(stringValue);
        String path = getPath(stringValue);

        if (dst < minDistance) {
          minDistance = dst;
          shortestPath = path;
        }
      }
    }
    
    if (!node.getDistanceFromSource().equals(minDistance)) {
      context.getCounter(DijkstraCounters.CHANGED_NODES).increment(1);
    }

    node.setDistanceFromSource(minDistance);
    node.setShortestPath(shortestPath);
    
    row.set(node.toString());
    context.write(NullWritable.get(), row);
  }

  private boolean isNode(String val) {
    return val.startsWith(NODE_VALUE_IDENTIFIER);
  }

  private Integer getDistance(String val) {
    String dst = val.split("-")[0];
    return INFINITE.equals(dst) ? INFINITE_VALUE : Integer.parseInt(dst);
  }

  private String getPath(String stringValue) {
    return stringValue.contains("-") ? stringValue.split("-")[1] : "";
  }

}
