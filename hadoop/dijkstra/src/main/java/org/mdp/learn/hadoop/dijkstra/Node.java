package org.mdp.learn.hadoop.dijkstra;

import static org.mdp.learn.hadoop.dijkstra.DijkstraConstants.INFINITE_VALUE;

import java.util.ArrayList;
import java.util.List;

public class Node {
  private String     id;
  private List<Edge> edges;
  private Integer    distanceFromSource;
  private String     shortestPath;

  public Node(String nodeId) {
    this.id = nodeId;
    this.edges = new ArrayList<>();
    this.shortestPath = "";
  }

  @Override
  public String toString() {
    return NodeTextTranslator.toString(this);
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

  public boolean isDistanceFromSourceInfinite() {
    return INFINITE_VALUE.equals(distanceFromSource);
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
