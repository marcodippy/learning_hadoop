package org.mdp.learn.hadoop.dijkstra;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import static org.mdp.learn.hadoop.dijkstra.DijkstraConstants.*;

public class DijkstraMapper extends Mapper<LongWritable, Text, Text, Text> {

  private Text nodeId = new Text();
  private Text val    = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String textValue = value.toString();
    Node node = NodeTextTranslator.parse(textValue);

    emitNodeAsIs(context, textValue, node);

    for (Edge edge : node.getEdges()) {
      emitEdge(context, node, edge);
    }
  }

  private void emitNodeAsIs(Context context, String textValue, Node node) throws IOException, InterruptedException {
    nodeId.set(node.getId());
    val.set(NODE_VALUE_IDENTIFIER + textValue);
    context.write(nodeId, val);
  }

  private void emitEdge(Context context, Node node, Edge edge) throws IOException, InterruptedException {
    nodeId.set(edge.getDestination().getId());

    if (node.isDistanceFromSourceInfinite()) {
      val.set(INFINITE);
    }
    else {
      String shortestPath = getCurrentShortestPath(node) + "/" + edge.getDestination().getId();
      Integer distanceFromSource = node.getDistanceFromSource() + edge.getWeight();
      val.set(distanceFromSource + "-" + shortestPath);
    }

    context.write(nodeId, val);
  }

  private String getCurrentShortestPath(Node node) {
    return node.getShortestPath().isEmpty() ? "/" + node.getId() : node.getShortestPath();
  }

}
