package org.mdp.learn.hadoop.moving_average;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovingAverageReducer extends Reducer<MovingAverageKey, TimeSeriesData, Text, Text> {

  private MovingAverage movingAverage;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    int windowSize = Integer.parseInt(context.getConfiguration().get("WindowSize", "4"));
    movingAverage = new MovingAverage(windowSize);
  }

  @Override
  protected void reduce(MovingAverageKey flight, Iterable<TimeSeriesData> timepoints, Context context) throws IOException, InterruptedException {

    for (TimeSeriesData data : timepoints) {
      movingAverage.addPrice(data.getPrice());

      context.write(formatKey(flight), getValue(data));
    }
  }

  private Text getValue(TimeSeriesData data) {
    String date = new SimpleDateFormat("dd/MM/yy").format(new Date(data.getTimestamp()));
    String averagePrice = String.format("%.2f", movingAverage.getMovingAverage());
    return new Text(date + "; " + averagePrice);
  }

  private Text formatKey(MovingAverageKey key) {
    return new Text(key.getDepartureAirport() + " - " + key.getArrivalAirport());
  }

}
