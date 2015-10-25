package org.mdp.learn.hadoop.top_n_records;

import java.util.PriorityQueue;
import java.util.Queue;

public class TopNQueue {
  private final Queue<Booking> queue;
  private final int N;

  public TopNQueue(int N) {
    queue = new PriorityQueue<>(N, (b1, b2) -> b1.getCount().compareTo(b2.getCount()));
    this.N = N;
  }

  public void addBooking(Booking booking) {
    queue.add(new Booking(booking));
    if (queue.size() > N) {
      queue.remove();
    }
  }

  public Queue<Booking> getTopN() {
    return queue;
  }

  public int getN() {
    return N;
  }

}
