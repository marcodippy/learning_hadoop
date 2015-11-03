package org.mdp.learn.hadoop.dijkstra;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class DijkstraTest {
  private MapDriver<LongWritable, Text, Text, Text>                           mapDriver;
  private MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> mapRedDriver;
  private Counters                                                            counters;

  @Before
  public void setUp() throws Exception {
    DijkstraMapper mapper = new DijkstraMapper();
    mapDriver = MapDriver.newMapDriver(mapper);

    DijkstraReducer reducer = new DijkstraReducer();
    
    mapRedDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

    counters = new Counters();
    mapRedDriver.withCounters(counters);
  }

  @Test
  public void testMap1() throws Exception {
    mapRedDriver.withInput(new LongWritable(0), new Text("MPX-[(JFK; 190),(HAV; 10)]-0"));
    mapRedDriver.withInput(new LongWritable(0), new Text("JFK-[(MIA; 60)]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("HAV-[(MIA; 10)]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("MIA-[(JFK; 10)]-INF"));

    mapRedDriver.run().forEach(System.out::println);

    setUp();

    System.out.println("******************");

    mapRedDriver.withInput(new LongWritable(0), new Text("HAV-[(MIA; 10)]-10-/MPX/HAV"));
    mapRedDriver.withInput(new LongWritable(0), new Text("JFK-[(MIA; 60)]-190-/MPX/JFK"));
    mapRedDriver.withInput(new LongWritable(0), new Text("MIA-[(JFK; 10)]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("MPX-[(JFK; 190),(HAV; 10)]-0"));

    mapRedDriver.run().forEach(System.out::println);

    setUp();

    System.out.println("******************");

    mapRedDriver.withInput(new LongWritable(0), new Text("HAV-[(MIA; 10)]-10-/MPX/HAV"));
    mapRedDriver.withInput(new LongWritable(0), new Text("JFK-[(MIA; 60)]-190-/MPX/JFK"));
    mapRedDriver.withInput(new LongWritable(0), new Text("MIA-[(JFK; 10)]-20-/MPX/HAV/MIA"));
    mapRedDriver.withInput(new LongWritable(0), new Text("MPX-[(JFK; 190),(HAV; 10)]-0"));

    mapRedDriver.run().forEach(System.out::println);

    setUp();
  }

}
