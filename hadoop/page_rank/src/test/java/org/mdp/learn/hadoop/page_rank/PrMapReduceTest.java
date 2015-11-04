package org.mdp.learn.hadoop.page_rank;

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

public class PrMapReduceTest {
  private MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> mapRedDriver;

  @Before
  public void setUp() throws Exception {
    mapRedDriver = MapReduceDriver.newMapReduceDriver(new PrMapper(), new PrReducer());
    mapRedDriver.withCounters(new Counters());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testJob() throws Exception {
    mapRedDriver.withInput(new LongWritable(0), new Text("n1-[n2, n4]-0.2"));
    mapRedDriver.withInput(new LongWritable(0), new Text("n2-[n3, n5]-0.2"));
    mapRedDriver.withInput(new LongWritable(0), new Text("n3-[n4]-0.2"));
    mapRedDriver.withInput(new LongWritable(0), new Text("n4-[n5]-0.2"));
    mapRedDriver.withInput(new LongWritable(0), new Text("n5-[n1, n2, n3]-0.2"));

    List<Pair<NullWritable, Text>> ret = mapRedDriver.run();

    ret.forEach(System.out::println);
    System.out.println("");
    setUp();
    List<Pair<LongWritable, Text>> inputs = ret.stream().map(x -> new Pair<LongWritable, Text>(new LongWritable(0), x.getSecond()))
        .collect(Collectors.toList());
    ret = mapRedDriver.withAll(inputs).run();
    ret.forEach(System.out::println);
  }

}
