package org.mdp.learn.hadoop.dijkstra;

import static org.mdp.learn.hadoop.dijkstra.DijkstraConstants.INFINITE;
import static org.mdp.learn.hadoop.dijkstra.DijkstraConstants.INFINITE_VALUE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public abstract class NodeTextTranslator {

  private static final int NODEID_INDEX        = 0;
  private static final int EDGES_INDEX         = 1;
  private static final int DISTANCE_INDEX      = 2;
  private static final int SHORTEST_PATH_INDEX = 3;

  public static Node parse(String textNode) {
    final String[] fields = textNode.toString().split("-");
    final Node n = new Node(fields[NODEID_INDEX]);

    n.setDistanceFromSource(INFINITE.equals(fields[DISTANCE_INDEX]) ? INFINITE_VALUE : Integer.parseInt(fields[DISTANCE_INDEX]));
    n.setEdges(getEdges(fields));
    n.setShortestPath(isShortestPathPresent(fields) ? fields[SHORTEST_PATH_INDEX].trim() : "");

    return n;
  }

  private static boolean isShortestPathPresent(String[] fields) {
    return fields.length >= SHORTEST_PATH_INDEX + 1;
  }

  private static List<Edge> getEdges(String[] fields) {
    String str = fields[EDGES_INDEX].replace("[", "").replace("]", "");
    return str.isEmpty() ? new ArrayList<>() : Arrays.asList(str.split(",")).stream().map(Edge::parseEdge).collect(Collectors.toList());
  }

  public static String toString(Node node) {
    StringBuilder sb = new StringBuilder(node.getId());

    sb.append("-").append(edgesToString(node));
    sb.append("-").append(distanceToString(node));
    
    if (node.getShortestPath() != null && !node.getShortestPath().isEmpty())
      sb.append("-").append(node.getShortestPath());

    return sb.toString();
  }

  private static String edgesToString(Node node) {
    return "[" + node.getEdges().stream().map(x -> x.toString()).collect(Collectors.joining(",")) + "]";
  }

  private static String distanceToString(Node node) {
    return node.isDistanceFromSourceInfinite() ? INFINITE : node.getDistanceFromSource().toString();
  }

}
