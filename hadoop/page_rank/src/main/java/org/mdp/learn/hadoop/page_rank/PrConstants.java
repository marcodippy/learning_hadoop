package org.mdp.learn.hadoop.page_rank;

public abstract class PrConstants {
  public static final String NODE_VALUE_IDENTIFIER = "VALUE=";
  public static final int PRECISION = 1000;
  
  public enum PrCounters {
    CHANGED_PAGE_RANKS, LOST_PAGE_RANK_MASS, NUMBER_OF_NODES
  }
}
