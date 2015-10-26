package org.mdp.learn.hadoop.moving_average;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovingAverageReducer extends Reducer<MovingAverageKey, TimeSeriesData, Text, Text> {

  private int WINDOW_SIZE;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    WINDOW_SIZE = Integer.parseInt(context.getConfiguration().get("WindowSize", "3"));
  }

  @Override
  protected void reduce(MovingAverageKey flight, Iterable<TimeSeriesData> timepoints, Context context) throws IOException, InterruptedException {
    MovingAverage movingAverage = new MovingAverage(WINDOW_SIZE);

    for (TimeSeriesData data : timepoints) {
      movingAverage.addPrice(data.getPrice());
      context.write(formatKey(flight), getValue(movingAverage, data));
    }
  }

  private Text getValue(MovingAverage movingAverage, TimeSeriesData data) {
    String date = new SimpleDateFormat("dd/MM/yy").format(new Date(data.getTimestamp()));
    String averagePrice = String.format("%.2f", movingAverage.getMovingAverage());
    return new Text(date + "; " + averagePrice);
  }

  private Text formatKey(MovingAverageKey key) {
    return new Text(key.getDepartureAirport() + " - " + key.getArrivalAirport());
  }

}
