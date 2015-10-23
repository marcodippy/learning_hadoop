package org.mdp.learn.hadoop.moving_average;

import java.io.IOException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class MovingAverageTest {
  private MapDriver<LongWritable, Text, MovingAverageKey, TimeSeriesData>                   mapDriver;
  private ReduceDriver<MovingAverageKey, TimeSeriesData, Text, Text>                        reducerDriver;
  private MapReduceDriver<LongWritable, Text, MovingAverageKey, TimeSeriesData, Text, Text> mapRedDriver;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    MovingAverageMapper mapper = new MovingAverageMapper();
    mapDriver = MapDriver.newMapDriver(mapper);

    MovingAverageReducer reducer = new MovingAverageReducer();
    reducerDriver = ReduceDriver.newReduceDriver(reducer);
    reducerDriver.getConfiguration().set("WindowSize", "3");

    mapRedDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

    mapRedDriver.setKeyOrderComparator(new MovingAverageKeyComparator());
    mapRedDriver.setKeyGroupingComparator(new MovingAverageKeyGroupingComparator());
    mapRedDriver.getConfiguration().set("WindowSize", "3");

  }

  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new LongWritable(1), new Text("Milan;New York;01/10/15;400"));
    mapDriver.withInput(new LongWritable(2), new Text("Milan;New York;01/10/15;440"));
    mapDriver.withInput(new LongWritable(3), new Text("Milan;New York;01/10/15;420"));

    long timestamp = new GregorianCalendar(2015, 9, 1).getTimeInMillis();

    mapDriver.withOutput(new MovingAverageKey("Milan", "New York", timestamp), new TimeSeriesData(timestamp, 400));
    mapDriver.withOutput(new MovingAverageKey("Milan", "New York", timestamp), new TimeSeriesData(timestamp, 440));
    mapDriver.withOutput(new MovingAverageKey("Milan", "New York", timestamp), new TimeSeriesData(timestamp, 420));

    mapDriver.runTest();
  }

  @Test
  public void testReducer() throws IOException {
    List<TimeSeriesData> inputs = new ArrayList<>();

    inputs.add(new TimeSeriesData(new GregorianCalendar(2015, 9, 1).getTimeInMillis(), 400));
    inputs.add(new TimeSeriesData(new GregorianCalendar(2015, 9, 2).getTimeInMillis(), 440));
    inputs.add(new TimeSeriesData(new GregorianCalendar(2015, 9, 3).getTimeInMillis(), 450));
    inputs.add(new TimeSeriesData(new GregorianCalendar(2015, 9, 4).getTimeInMillis(), 430));
    inputs.add(new TimeSeriesData(new GregorianCalendar(2015, 9, 5).getTimeInMillis(), 496));
    inputs.add(new TimeSeriesData(new GregorianCalendar(2015, 9, 6).getTimeInMillis(), 510));
    inputs.add(new TimeSeriesData(new GregorianCalendar(2015, 9, 7).getTimeInMillis(), 398));
    inputs.add(new TimeSeriesData(new GregorianCalendar(2015, 9, 8).getTimeInMillis(), 405));
    inputs.add(new TimeSeriesData(new GregorianCalendar(2015, 9, 9).getTimeInMillis(), 516));
    inputs.add(new TimeSeriesData(new GregorianCalendar(2015, 9, 10).getTimeInMillis(), 548));
    inputs.add(new TimeSeriesData(new GregorianCalendar(2015, 9, 11).getTimeInMillis(), 579));
    inputs.add(new TimeSeriesData(new GregorianCalendar(2015, 9, 12).getTimeInMillis(), 613));

    reducerDriver.withInput(new MovingAverageKey("Milan", "New York", 0), inputs);

    Text key = new Text("Milan - New York");

    reducerDriver.withOutput(key, new Text("01/10/15; 400.00"));
    reducerDriver.withOutput(key, new Text("02/10/15; 420.00"));
    reducerDriver.withOutput(key, new Text("03/10/15; 430.00"));
    reducerDriver.withOutput(key, new Text("04/10/15; 440.00"));
    reducerDriver.withOutput(key, new Text("05/10/15; 458.67"));
    reducerDriver.withOutput(key, new Text("06/10/15; 478.67"));
    reducerDriver.withOutput(key, new Text("07/10/15; 468.00"));
    reducerDriver.withOutput(key, new Text("08/10/15; 437.67"));
    reducerDriver.withOutput(key, new Text("09/10/15; 439.67"));
    reducerDriver.withOutput(key, new Text("10/10/15; 489.67"));
    reducerDriver.withOutput(key, new Text("11/10/15; 547.67"));
    reducerDriver.withOutput(key, new Text("12/10/15; 580.00"));

    reducerDriver.runTest();
  }

  @Test
  public void testMapReduce() throws IOException {
    mapRedDriver.withInput(new LongWritable(1), new Text("Milan;New York;01/10/15;400"));
    mapRedDriver.withInput(new LongWritable(2), new Text("Milan;New York;02/10/15;440"));
    mapRedDriver.withInput(new LongWritable(3), new Text("Milan;New York;03/10/15;450"));
    mapRedDriver.withInput(new LongWritable(4), new Text("Milan;New York;04/10/15;430"));
    mapRedDriver.withInput(new LongWritable(5), new Text("Milan;New York;05/10/15;496"));
    mapRedDriver.withInput(new LongWritable(6), new Text("Milan;New York;06/10/15;510"));
    mapRedDriver.withInput(new LongWritable(7), new Text("Milan;New York;07/10/15;398"));
    mapRedDriver.withInput(new LongWritable(8), new Text("Milan;New York;08/10/15;405"));
    mapRedDriver.withInput(new LongWritable(9), new Text("Milan;New York;09/10/15;516"));
    mapRedDriver.withInput(new LongWritable(10), new Text("Milan;New York;10/10/15;548"));
    mapRedDriver.withInput(new LongWritable(11), new Text("Milan;New York;11/10/15;579"));
    mapRedDriver.withInput(new LongWritable(12), new Text("Milan;New York;12/10/15;613"));

    Text key = new Text("Milan - New York");

    mapRedDriver.withOutput(key, new Text("01/10/15; 400.00"));
    mapRedDriver.withOutput(key, new Text("02/10/15; 420.00"));
    mapRedDriver.withOutput(key, new Text("03/10/15; 430.00"));
    mapRedDriver.withOutput(key, new Text("04/10/15; 440.00"));
    mapRedDriver.withOutput(key, new Text("05/10/15; 458.67"));
    mapRedDriver.withOutput(key, new Text("06/10/15; 478.67"));
    mapRedDriver.withOutput(key, new Text("07/10/15; 468.00"));
    mapRedDriver.withOutput(key, new Text("08/10/15; 437.67"));
    mapRedDriver.withOutput(key, new Text("09/10/15; 439.67"));
    mapRedDriver.withOutput(key, new Text("10/10/15; 489.67"));
    mapRedDriver.withOutput(key, new Text("11/10/15; 547.67"));
    mapRedDriver.withOutput(key, new Text("12/10/15; 580.00"));

    mapRedDriver.runTest(true);
  }
}
