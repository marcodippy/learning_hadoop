package org.mdp.learn.hadoop.page_rank;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
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
import org.mdp.learn.hadoop.page_rank.PrConstants.PrCounters;

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
    List<Pair<LongWritable, Text>> inputs = Arrays.asList(
        new Pair<>(new LongWritable(0), new Text("n1;[n2, n4];0.2")),
        new Pair<>(new LongWritable(0), new Text("n2;[n3, n5];0.2")),
        new Pair<>(new LongWritable(0), new Text("n3;[n4];0.2")),
        new Pair<>(new LongWritable(0), new Text("n4;[n5];0.2")),
        new Pair<>(new LongWritable(0), new Text("n5;[n1, n2, n3];0.2"))
    );

    List<Pair<NullWritable, Text>> ret = null;
    long changedPageRanks = 1;
    int iterations = 0;
    
    do {
      ret = mapRedDriver.withAll(inputs).run();
      inputs = ret.stream().map(x -> new Pair<LongWritable, Text>(new LongWritable(0), x.getSecond())).collect(Collectors.toList());

      changedPageRanks = mapRedDriver.getCounters().findCounter(PrCounters.CHANGED_PAGE_RANKS).getValue();
      ret.forEach(System.out::println);
      System.out.println("");
      iterations++;
      setUp();
    }
    while (changedPageRanks > 0);

    System.out.println("MapReduce finished in " + iterations + " iterations");
  }

}
