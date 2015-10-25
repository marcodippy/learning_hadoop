package org.mdp.learn.hadoop.moving_average;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovingAverageMapper extends Mapper<LongWritable, Text, MovingAverageKey, TimeSeriesData> {

  private int DEPARTURE_AIRPORT_INDEX, ARRIVAL_AIRPORT_INDEX, DATE_INDEX, PRICE_INDEX;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    DEPARTURE_AIRPORT_INDEX = Integer.parseInt(context.getConfiguration().get("departureAirport_index"));
    ARRIVAL_AIRPORT_INDEX = Integer.parseInt(context.getConfiguration().get("arrivalAirport_index"));
    DATE_INDEX = Integer.parseInt(context.getConfiguration().get("timestamp_index"));
    PRICE_INDEX = Integer.parseInt(context.getConfiguration().get("price_index"));
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] values = value.toString().split(";");
    context.write(getKey(values), getTimeSeriesData(values));
  }

  private TimeSeriesData getTimeSeriesData(String[] values) {
    return new TimeSeriesData(getTimestamp(values), Double.parseDouble(values[PRICE_INDEX].trim()));
  }

  private MovingAverageKey getKey(String[] values) {
    String departureAirport = values[DEPARTURE_AIRPORT_INDEX].trim();
    String arrivalAirport = values[ARRIVAL_AIRPORT_INDEX].trim();
    long timestamp = getTimestamp(values);

    return new MovingAverageKey(departureAirport, arrivalAirport, timestamp);
  }

  private long getTimestamp(String[] values) {
    long timestamp = 0;
    try {
      timestamp = new SimpleDateFormat("dd/MM/yy").parse(values[DATE_INDEX].trim()).getTime();
    }
    catch (ParseException e) {
      e.printStackTrace();
    }
    return timestamp;
  }

}
