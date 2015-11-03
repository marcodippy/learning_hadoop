package org.mdp.learn.hadoop.dijkstra;

public class Edge {
  private Node    destination;
  private Integer weight;

  public Edge(Node destination, Integer weight) {
    this.destination = destination;
    this.weight = weight;
  }

  /**
   * it takes a string like (nodeId;weight)
   */
  public static Edge parseEdge(String str) {
    String[] vals = str.replace("(", "").replace(")", "").split(";");
    return new Edge(new Node(vals[0].trim()), Integer.parseInt(vals[1].trim()));
  }

  public Node getDestination() {
    return destination;
  }

  public Integer getWeight() {
    return weight;
  }

  public void setWeight(Integer weight) {
    this.weight = weight;
  }

  @Override
  public String toString() {
    return "(" + destination.getId() + "; " + weight + ")";
  }

}
