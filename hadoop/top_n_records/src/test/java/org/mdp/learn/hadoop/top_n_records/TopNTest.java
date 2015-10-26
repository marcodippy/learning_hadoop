package org.mdp.learn.hadoop.top_n_records;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class TopNTest {

  private MapDriver<LongWritable, Text, NullWritable, Booking> mapDriver;
  private ReduceDriver<NullWritable, Booking, Text, IntWritable> reducerDriver;
  private MapReduceDriver<LongWritable, Text, NullWritable, Booking, Text, IntWritable> mapRedDriver;
  private Path inputFile;
  
  @Before
  public void setUp() throws Exception {
    TopNMapper mapper = new TopNMapper();
    mapDriver = MapDriver.newMapDriver(mapper);

    TopNReducer reducer = new TopNReducer();
    reducerDriver = ReduceDriver.newReduceDriver(reducer);

    mapRedDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    
    inputFile = Paths.get(getClass().getResource("/input_top_n_records").toURI());
  }

  @Test
  public void testMapper() throws IOException, URISyntaxException {
    mapDriver.getConfiguration().set("top.n", "4");
    mapDriver.addAll(TestUtils.getInput(inputFile));
    
    mapDriver.addAllOutput(
        Arrays.asList(
            new Pair<>(NullWritable.get(), new Booking("Milan-London", 10000)),
            new Pair<>(NullWritable.get(), new Booking("San Francisco-Madrid", 9999)),
            new Pair<>(NullWritable.get(), new Booking("New York-Dubai", 9998)),
            new Pair<>(NullWritable.get(), new Booking("Havana-Madrid", 9997))
            )
        );
    
    mapDriver.runTest(false);
  }

  @Test
  public void testReducer() throws IOException, URISyntaxException {
    reducerDriver.getConfiguration().set("top.n", "4");
    
    List<Pair<LongWritable, Text>> mapInputs = TestUtils.getInput(inputFile);
   
    List<Booking> redInputs = mapInputs.stream()
      .map(pair -> pair.getSecond().toString())
      .map(line -> line.split(";"))
      .map(fields -> new Booking(fields[0], fields[1], Integer.parseInt(fields[2])))
      .collect(Collectors.toList());
    
    reducerDriver.withInput(new Pair<>(NullWritable.get(), redInputs));
    
    reducerDriver.addAllOutput(
        Arrays.asList(
            new Pair<>(new Text("Milan-London"), new IntWritable(10000)),
            new Pair<>(new Text("San Francisco-Madrid"), new IntWritable(9999)),
            new Pair<>(new Text("New York-Dubai"), new IntWritable(9998)),
            new Pair<>(new Text("Havana-Madrid"), new IntWritable(9997))
            )
        );
    
    reducerDriver.runTest(true);
  }
  
  @Test
  public void testMapReduce() throws IOException, URISyntaxException {
    mapRedDriver.getConfiguration().set("top.n", "4");
    mapRedDriver.addAll(TestUtils.getInput(inputFile));

    mapRedDriver.addAllOutput(
        Arrays.asList(
            new Pair<>(new Text("Milan-London"), new IntWritable(10000)),
            new Pair<>(new Text("San Francisco-Madrid"), new IntWritable(9999)),
            new Pair<>(new Text("New York-Dubai"), new IntWritable(9998)),
            new Pair<>(new Text("Havana-Madrid"), new IntWritable(9997))
            )
        );
    
    mapRedDriver.runTest(true);
  }
}
