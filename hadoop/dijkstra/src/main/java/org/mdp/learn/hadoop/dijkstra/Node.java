package org.mdp.learn.hadoop.dijkstra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Node {
  private String     id;
  private List<Edge> edges        = new ArrayList<>();
  private Integer    distanceFromSource;
  private String     shortestPath = "";

  public Node(String nodeId) {
    this.id = nodeId;
  }

  public static Node parseNode(String s) {
    String[] fields = s.toString().split("-");

    Node n = new Node(fields[0]);
    n.setDistanceFromSource("INF".equals(fields[2]) ? Integer.MAX_VALUE : Integer.parseInt(fields[2]));
    n.setEdges(getEdges(fields));
    if (fields.length == 4) n.setShortestPath(fields[3].trim());

    return n;
  }

  private static List<Edge> getEdges(String[] fields) {
    String str = fields[1].replace("[", "").replace("]", "");
    return str.isEmpty() ? new ArrayList<>() : Arrays.asList(str.split(",")).stream().map(Edge::parseEdge).collect(Collectors.toList());
  }

  @Override
  public String toString() {
    String dst = distanceFromSource.toString();
    if (distanceFromSource.equals(Integer.MAX_VALUE)) dst = "INF";
    return id + "-[" + edges.stream().map(x -> x.toString()).collect(Collectors.joining(",")) + "]-" + dst + (shortestPath.isEmpty() ? "" : "-"+shortestPath);
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Integer getDistanceFromSource() {
    return distanceFromSource;
  }

  public void setDistanceFromSource(Integer distanceFromSource) {
    this.distanceFromSource = distanceFromSource;
  }

  public List<Edge> getEdges() {
    return edges;
  }

  public void setEdges(List<Edge> edges) {
    this.edges = edges;
  }

  public String getShortestPath() {
    return shortestPath;
  }

  public void setShortestPath(String shortestPath) {
    this.shortestPath = shortestPath;
  }

  public void addNodeToPath(String nodeId) {
    this.shortestPath += nodeId;
  }

}
