package org.mdp.learn.hadoop.page_rank;

import java.util.ArrayList;
import java.util.List;

public class Node {
  private String       id;
  private List<String> links;
  private Double       pageRank;

  public Node(String nodeId) {
    this.id = nodeId;
    this.links = new ArrayList<>();
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

  public List<String> getLinks() {
    return links;
  }

  public void setLinks(List<String> links) {
    this.links = links;
  }

  public Double getPageRank() {
    return pageRank;
  }

  public void setPageRank(Double pageRank) {
    this.pageRank = pageRank;
  }

}
