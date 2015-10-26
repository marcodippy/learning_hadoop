package org.mdp.learn.hadoop.moving_average;

import java.util.LinkedList;
import java.util.Queue;

public class MovingAverage {

  private double              sum    = 0.0;
  private final int           windowSize;
  private final Queue<Double> window = new LinkedList<>();

  public MovingAverage(int windowSize) {
    this.windowSize = windowSize;
  }

  public void addPrice(double price) {
    sum += price;
    window.add(price);

    if (window.size() > windowSize) {
      sum -= window.remove();
    }
  }

  public double getMovingAverage() {
    return sum / window.size();
  }

  public void clean() {
    window.clear();
    sum = 0.0;
  }
}
