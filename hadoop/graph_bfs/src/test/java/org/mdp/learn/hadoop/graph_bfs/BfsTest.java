package org.mdp.learn.hadoop.graph_bfs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;

public class BfsTest {
  private MapDriver<LongWritable, Text, LongWritable, Text>                           mapDriver;
  private ReduceDriver<LongWritable, Text, NullWritable, Text>                        reducerDriver;
  private MapReduceDriver<LongWritable, Text, LongWritable, Text, NullWritable, Text> mapRedDriver;

  @Before
  public void setUp() throws Exception {
    BfsMapper mapper = new BfsMapper();
    mapDriver = MapDriver.newMapDriver(mapper);

    BfsReducer reducer = new BfsReducer();
    reducerDriver = ReduceDriver.newReduceDriver(reducer);

    mapRedDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }


  
  public void testMap() throws IOException {
    System.out.println(Integer.MAX_VALUE);
    mapDriver.withInput(new LongWritable(0), new Text("1-[2,3,6]-0"));
    mapDriver.withInput(new LongWritable(0), new Text("2-[4]-INF"));
    mapDriver.withInput(new LongWritable(0), new Text("3-[4,5]-INF"));
    mapDriver.withInput(new LongWritable(0), new Text("4-[5]-INF"));
    mapDriver.withInput(new LongWritable(0), new Text("5-[6]-INF"));

    mapDriver.withOutput(new LongWritable(1), new Text("1-[2,3]-0"));
    mapDriver.withOutput(new LongWritable(2), new Text("1"));
    mapDriver.withOutput(new LongWritable(3), new Text("1"));

    mapDriver.run().forEach(System.out::println);
  }

  public void testMapReduce() throws IOException {
    mapRedDriver.withInput(new LongWritable(0), new Text("1-[2,3,6]-0"));
    mapRedDriver.withInput(new LongWritable(0), new Text("2-[4]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("3-[4,5]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("4-[5]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("5-[6]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("6-[]-INF"));

    mapRedDriver.run().forEach(System.out::println);
  }
  
  public void testMapReduce2() throws IOException {
    mapRedDriver.withInput(new LongWritable(0), new Text("1-[2,3,6]-0"));
    mapRedDriver.withInput(new LongWritable(0), new Text("2-[4]-1"));
    mapRedDriver.withInput(new LongWritable(0), new Text("3-[4,5]-1"));
    mapRedDriver.withInput(new LongWritable(0), new Text("4-[5]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("5-[6]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("6-[]-1"));

    mapRedDriver.run().forEach(System.out::println);
  }
  
  public void testMapReduce3() throws IOException {
    mapRedDriver.withInput(new LongWritable(0), new Text("1-[2,3,6]-0"));
    mapRedDriver.withInput(new LongWritable(0), new Text("2-[4]-1"));
    mapRedDriver.withInput(new LongWritable(0), new Text("3-[4,5]-1"));
    mapRedDriver.withInput(new LongWritable(0), new Text("4-[5]-2"));
    mapRedDriver.withInput(new LongWritable(0), new Text("5-[6]-2"));
    mapRedDriver.withInput(new LongWritable(0), new Text("6-[]-1"));

    mapRedDriver.run().forEach(System.out::println);
  }
  

}
