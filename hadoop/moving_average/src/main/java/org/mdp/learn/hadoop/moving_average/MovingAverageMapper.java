package org.mdp.learn.hadoop.moving_average;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovingAverageMapper extends Mapper<LongWritable, Text, MovingAverageKey, FloatWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] values = value.toString().split(";");
    context.write(getKey(values), new FloatWritable(Float.parseFloat(values[5].trim())));
  }

  private MovingAverageKey getKey(String[] values) {
    String departureAirport = values[1].trim();
    String arrivalAirport = values[2].trim();
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/YY");
    Long timestamp = null;
    try {
      timestamp = sdf.parse(values[3].trim()).getTime();

    } catch (ParseException e) {
      e.printStackTrace();
    }

    return new MovingAverageKey(departureAirport, arrivalAirport, timestamp);
  }

}
