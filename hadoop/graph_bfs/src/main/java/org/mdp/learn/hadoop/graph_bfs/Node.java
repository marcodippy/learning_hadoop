package org.mdp.learn.hadoop.graph_bfs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Node {
  private Long       nodeId;
  private List<Long> neighbors = new ArrayList<>();
  private Integer    distanceFromSource;

  public Node(Long nodeId) {
    this.nodeId = nodeId;
  }

  public static Node parseNode(String s) {
    String[] fields = s.toString().split("-");

    Node n = new Node(Long.parseLong(fields[0]));

    String dst = fields[2];
    n.setDistanceFromSource("INF".equals(dst) ? Integer.MAX_VALUE : Integer.parseInt(fields[2]));

    if (!fields[1].equals("[]"))
      n.setNeighbors(Arrays.asList(fields[1].replace("[", "").replace("]", "").split(",")).stream().map(Long::parseLong).collect(Collectors.toList()));

    return n;
  }

  @Override
  public String toString() {
    String dst = distanceFromSource.toString();
    if (distanceFromSource.equals(Integer.MAX_VALUE)) dst = "INF";
    return nodeId + "-[" + neighbors.stream().map(x -> x.toString()).collect(Collectors.joining(",")) + "]-" + dst;
  }

  public Long getNodeId() {
    return nodeId;
  }

  public void setNodeId(Long nodeId) {
    this.nodeId = nodeId;
  }

  public List<Long> getNeighbors() {
    return neighbors;
  }

  public void setNeighbors(List<Long> neighbors) {
    this.neighbors = neighbors;
  }

  public Integer getDistanceFromSource() {
    return distanceFromSource;
  }

  public void setDistanceFromSource(Integer distanceFromSource) {
    this.distanceFromSource = distanceFromSource;
  }

}
