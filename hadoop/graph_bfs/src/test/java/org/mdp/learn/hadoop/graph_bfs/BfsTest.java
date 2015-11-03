package org.mdp.learn.hadoop.graph_bfs;

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
import org.mdp.learn.hadoop.graph_bfs.not_weighted.BfsCounters;
import org.mdp.learn.hadoop.graph_bfs.not_weighted.BfsMapper;
import org.mdp.learn.hadoop.graph_bfs.not_weighted.BfsReducer;

public class BfsTest {
  private MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
  private MapReduceDriver<LongWritable, Text, LongWritable, Text, NullWritable, Text> mapRedDriver;
  private Counters counters;

  @Before
  public void setUp() throws Exception {
    BfsMapper mapper = new BfsMapper();
    mapDriver = MapDriver.newMapDriver(mapper);

    BfsReducer reducer = new BfsReducer();

    mapRedDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

    counters = new Counters();
    mapRedDriver.withCounters(counters);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testJob() throws Exception {
    mapRedDriver.withInput(new LongWritable(0), new Text("1-[2,3,6]-0"));
    mapRedDriver.withInput(new LongWritable(0), new Text("2-[4]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("3-[4,5]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("4-[5]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("5-[6]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("6-[7]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("7-[8]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("8-[]-INF"));

    List<Pair<NullWritable, Text>> ret = mapRedDriver.run();
    long notDiscoveredNodes = mapRedDriver.getCounters().findCounter(BfsCounters.NOT_DISCOVERED_NODES).getValue();

    do {
      setUp();
      List<Pair<LongWritable, Text>> inputs = ret.stream()
          .map(x -> new Pair<LongWritable, Text>(new LongWritable(0), x.getSecond())).collect(Collectors.toList());
      ret = mapRedDriver.withAll(inputs).run();
      notDiscoveredNodes = mapRedDriver.getCounters().findCounter(BfsCounters.NOT_DISCOVERED_NODES).getValue();
    } while (notDiscoveredNodes > 0);

    assertThat(ret).containsExactly(new Pair<>(NullWritable.get(), new Text("1-[2,3,6]-0")),
        new Pair<>(NullWritable.get(), new Text("2-[4]-1")), new Pair<>(NullWritable.get(), new Text("3-[4,5]-1")),
        new Pair<>(NullWritable.get(), new Text("4-[5]-2")), new Pair<>(NullWritable.get(), new Text("5-[6]-2")),
        new Pair<>(NullWritable.get(), new Text("6-[7]-1")), new Pair<>(NullWritable.get(), new Text("7-[8]-2")),
        new Pair<>(NullWritable.get(), new Text("8-[]-3")));
  }

  @Test
  public void testMap() throws IOException {
    mapDriver.withInput(new LongWritable(0), new Text("1-[2,3,6]-0"));
    mapDriver.withInput(new LongWritable(0), new Text("2-[4]-INF"));
    mapDriver.withInput(new LongWritable(0), new Text("3-[4,5]-INF"));
    mapDriver.withInput(new LongWritable(0), new Text("4-[5]-INF"));
    mapDriver.withInput(new LongWritable(0), new Text("5-[6]-INF"));
    mapDriver.withInput(new LongWritable(0), new Text("6-[]-INF"));

    mapDriver.withOutput(new LongWritable(1), new Text("VALUE=1-[2,3,6]-0"));
    mapDriver.withOutput(new LongWritable(2), new Text("1"));
    mapDriver.withOutput(new LongWritable(3), new Text("1"));
    mapDriver.withOutput(new LongWritable(6), new Text("1"));
    mapDriver.withOutput(new LongWritable(2), new Text("VALUE=2-[4]-INF"));
    mapDriver.withOutput(new LongWritable(4), new Text("INF"));
    mapDriver.withOutput(new LongWritable(3), new Text("VALUE=3-[4,5]-INF"));
    mapDriver.withOutput(new LongWritable(4), new Text("INF"));
    mapDriver.withOutput(new LongWritable(5), new Text("INF"));
    mapDriver.withOutput(new LongWritable(4), new Text("VALUE=4-[5]-INF"));
    mapDriver.withOutput(new LongWritable(5), new Text("INF"));
    mapDriver.withOutput(new LongWritable(5), new Text("VALUE=5-[6]-INF"));
    mapDriver.withOutput(new LongWritable(6), new Text("INF"));
    mapDriver.withOutput(new LongWritable(6), new Text("VALUE=6-[]-INF"));

    mapDriver.runTest();
  }

  @Test
  public void testMapReduce() throws IOException {
    mapRedDriver.withInput(new LongWritable(0), new Text("1-[2,3,6]-0"));
    mapRedDriver.withInput(new LongWritable(0), new Text("2-[4]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("3-[4,5]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("4-[5]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("5-[6]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("6-[]-INF"));

    mapRedDriver.withOutput(NullWritable.get(), new Text("1-[2,3,6]-0"));
    mapRedDriver.withOutput(NullWritable.get(), new Text("2-[4]-1"));
    mapRedDriver.withOutput(NullWritable.get(), new Text("3-[4,5]-1"));
    mapRedDriver.withOutput(NullWritable.get(), new Text("4-[5]-INF"));
    mapRedDriver.withOutput(NullWritable.get(), new Text("5-[6]-INF"));
    mapRedDriver.withOutput(NullWritable.get(), new Text("6-[]-1"));

    mapRedDriver.runTest();
  }

  @Test
  public void testMapReduce2() throws IOException {
    mapRedDriver.withInput(new LongWritable(0), new Text("1-[2,3,6]-0"));
    mapRedDriver.withInput(new LongWritable(0), new Text("2-[4]-1"));
    mapRedDriver.withInput(new LongWritable(0), new Text("3-[4,5]-1"));
    mapRedDriver.withInput(new LongWritable(0), new Text("4-[5]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("5-[6]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("6-[]-1"));

    mapRedDriver.withOutput(NullWritable.get(), new Text("1-[2,3,6]-0"));
    mapRedDriver.withOutput(NullWritable.get(), new Text("2-[4]-1"));
    mapRedDriver.withOutput(NullWritable.get(), new Text("3-[4,5]-1"));
    mapRedDriver.withOutput(NullWritable.get(), new Text("4-[5]-2"));
    mapRedDriver.withOutput(NullWritable.get(), new Text("5-[6]-2"));
    mapRedDriver.withOutput(NullWritable.get(), new Text("6-[]-1"));

    mapRedDriver.runTest();
  }

}
