package org.mdp.learn.hadoop.cwbtiab;

import java.util.PriorityQueue;
import java.util.Queue;

public class TopNQueue {
  private final Queue<Count> queue;
  private final int          N;

  public TopNQueue(int N) {
    queue = new PriorityQueue<>(N, (b1, b2) -> b1.getCount().compareTo(b2.getCount()));
    this.N = N;
  }

  public void addCount(Count count) {
    queue.add(new Count(count));
    if (queue.size() > N) {
      queue.remove();
    }
  }

  public Queue<Count> getTopN() {
    return queue;
  }

  public int getN() {
    return N;
  }

}