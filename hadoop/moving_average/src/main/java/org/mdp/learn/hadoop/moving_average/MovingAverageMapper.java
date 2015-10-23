package org.mdp.learn.hadoop.moving_average;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovingAverageMapper extends Mapper<LongWritable, Text, MovingAverageKey, TimeSeriesData> {

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] values = value.toString().split(";");
    context.write(getKey(values), getTimeSeriesData(values));
  }

  private TimeSeriesData getTimeSeriesData(String[] values) {
    return new TimeSeriesData(getTimestamp(values), Double.parseDouble(values[3].trim()));
  }

  private MovingAverageKey getKey(String[] values) {
    String departureAirport = values[0].trim();
    String arrivalAirport = values[1].trim();
    long timestamp = getTimestamp(values);

    return new MovingAverageKey(departureAirport, arrivalAirport, timestamp);
  }

  private long getTimestamp(String[] values) {
    long timestamp = 0;
    try {
      timestamp = new SimpleDateFormat("dd/MM/yy").parse(values[2].trim()).getTime();
    }
    catch (ParseException e) {
      e.printStackTrace();
    }
    return timestamp;
  }

}
