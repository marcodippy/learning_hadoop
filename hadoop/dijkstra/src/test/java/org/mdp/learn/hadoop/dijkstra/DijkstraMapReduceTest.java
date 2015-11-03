package org.mdp.learn.hadoop.dijkstra;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class DijkstraMapReduceTest {
  private MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> mapRedDriver;

  @Before
  public void setUp() throws Exception {
    mapRedDriver = MapReduceDriver.newMapReduceDriver(new DijkstraMapper(), new DijkstraReducer());
    mapRedDriver.withCounters(new Counters());
  }
 
  
  @SuppressWarnings("unchecked")
  @Test
  public void testJob() throws Exception {
    mapRedDriver.withInput(new LongWritable(0), new Text("MPX-[(JFK; 190),(HAV; 10)]-0"));
    mapRedDriver.withInput(new LongWritable(0), new Text("JFK-[(MIA; 60)]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("HAV-[(MIA; 10)]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("MIA-[(JFK; 10)]-INF"));

    int iterations = 0;

    List<Pair<NullWritable, Text>> ret = mapRedDriver.run();
    long changedNodes = mapRedDriver.getCounters().findCounter(DijkstraCounters.CHANGED_NODES).getValue();
    iterations++;
    
    do {
      setUp();
      List<Pair<LongWritable, Text>> inputs = ret.stream().map(x -> new Pair<LongWritable, Text>(new LongWritable(0), x.getSecond())).collect(Collectors.toList());
      ret = mapRedDriver.withAll(inputs).run();
      changedNodes = mapRedDriver.getCounters().findCounter(DijkstraCounters.CHANGED_NODES).getValue();
      iterations++;
    }
    while (changedNodes > 0);

    assertThat(ret).contains(
        new Pair<>(NullWritable.get(), new Text("HAV-[(MIA; 10)]-10-/MPX/HAV")),
        new Pair<>(NullWritable.get(), new Text("JFK-[(MIA; 60)]-30-/MPX/HAV/MIA/JFK")),
        new Pair<>(NullWritable.get(), new Text("MIA-[(JFK; 10)]-20-/MPX/HAV/MIA")), 
        new Pair<>(NullWritable.get(), new Text("MPX-[(JFK; 190),(HAV; 10)]-0")));
    
    assertThat(iterations).isEqualTo(4);
  }

}
