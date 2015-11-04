package org.mdp.learn.hadoop.page_rank;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public abstract class NodeTextTranslator {

  private static final int NODEID_INDEX = 0;
  private static final int LINKS_INDEX  = 1;
  private static final int PAGE_RANK    = 2;

  public static Node parse(String textNode) {
    final String[] fields = textNode.toString().split("-");

    final Node n = new Node(fields[NODEID_INDEX]);
    n.setLinks(getLinks(fields));
    n.setPageRank(Double.parseDouble(fields[PAGE_RANK].trim()));

    return n;
  }

  private static List<String> getLinks(String[] fields) {
    String str = fields[LINKS_INDEX].replace("[", "").replace("]", "");
    return str.isEmpty() ? new ArrayList<>() : Arrays.asList(str.split(",")).stream().map(String::trim).collect(Collectors.toList());
  }

  public static String toString(Node node) {
    StringBuilder sb = new StringBuilder(node.getId());

    sb.append("-").append(linksToString(node));
    sb.append("-").append(node.getPageRank());

    return sb.toString();
  }

  private static String linksToString(Node node) {
    return "[" + node.getLinks().stream().collect(Collectors.joining(",")) + "]";
  }

}
