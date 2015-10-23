package org.mdp.learn.hadoop.moving_average;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AveragePerDayReducer extends Reducer<MovingAverageKey, TimeSeriesData, Text, Text> {

  @Override
  protected void reduce(MovingAverageKey flight, Iterable<TimeSeriesData> timepoints, Context context) throws IOException, InterruptedException {
    int count = 0;
    double sum = 0;

    for (TimeSeriesData data : timepoints) {
      sum += data.getPrice();
      count++;
    }

    String date = new SimpleDateFormat("dd/MM/yy").format(new Date(flight.getTimestamp().get()));
    String averagePricePerDay = String.format("%.2f", (sum / count));
    context.write(formatKey(flight), new Text(date + ";" + averagePricePerDay));
  }

  private Text formatKey(MovingAverageKey key) {
    return new Text(key.getDepartureAirport() + ";" + key.getArrivalAirport());
  }

}
