package org.mdp.learn.hadoop.people_you_may_know;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mdp.learn.hadoop.commons.TextPair;

public class PymkTest {
  private MapReduceDriver<LongWritable, Text, Text, TextPair, Text, Text> mapRedDriver;

  @Before
  public void setUp() throws Exception {
    mapRedDriver = MapReduceDriver.newMapReduceDriver(new PymkMapper(), new PymkReducer());
  }

  @Test
  public void testMapReduce() throws IOException {
    mapRedDriver.withInput(new LongWritable(1), new Text("Marco-Fabio,Luca,Gordon"));
    mapRedDriver.withInput(new LongWritable(2), new Text("Fabio-Nathalia,Marco,Luca,Gordon"));
    mapRedDriver.withInput(new LongWritable(3), new Text("Luca-Nathalia,Marco,Fabio"));
    mapRedDriver.withInput(new LongWritable(4), new Text("Nathalia-Fabio,Luca"));
    mapRedDriver.withInput(new LongWritable(5), new Text("Gordon-Marco,Fabio"));

    mapRedDriver.run().forEach(System.out::println);
  }
}
