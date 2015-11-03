package org.mdp.learn.hadoop.dijkstra;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DijkstraMapper extends Mapper<LongWritable, Text, Text, Text> {

  private Text nodeId = new Text();
  private Text val    = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Node node = Node.parseNode(value.toString());

    nodeId.set(node.getId());
    val.set("VALUE=" + value.toString());
    context.write(nodeId, val);

    for (Edge edge : node.getEdges()) {
      nodeId.set(edge.getDestination().getId());
      String shortest = (node.getShortestPath().isEmpty() ? "/" + node.getId() : node.getShortestPath()) + "/" + edge.getDestination().getId();
      if (Integer.MAX_VALUE == node.getDistanceFromSource()) {
        val.set("INF");
      }
      else {
        val.set(node.getDistanceFromSource() + edge.getWeight() + "-" + shortest);
      }

      context.write(nodeId, val);
    }
  }

}
