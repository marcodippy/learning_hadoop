package org.mdp.learn.hadoop.frequently_bought_together;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class FBTTest {
  private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapRedDriver;

  @Before
  public void setUp() throws Exception {
    mapRedDriver = MapReduceDriver.newMapReduceDriver(new FBTMapper(), new FBTReducer());
  }

  @Test
  public void testMapReduce() throws IOException {
    mapRedDriver.withInput(new LongWritable(1), new Text("t1-p1,p2,p3"));
    mapRedDriver.withInput(new LongWritable(2), new Text("t2-p1,p2,p4,p5"));
    mapRedDriver.withInput(new LongWritable(3), new Text("t3-p3,p1,p2,p6"));
    mapRedDriver.withInput(new LongWritable(4), new Text("t4-p5,p2,p1"));
    mapRedDriver.withInput(new LongWritable(5), new Text("t5-p3,p1,p8,p7"));
    
    mapRedDriver.withAllOutput(Arrays.asList(
        new Pair<>(new Text("p1,p2"), new IntWritable(4)),
        new Pair<>(new Text("p1,p3"), new IntWritable(3)),
        new Pair<>(new Text("p1,p4"), new IntWritable(1)),
        new Pair<>(new Text("p1,p5"), new IntWritable(2)),
        new Pair<>(new Text("p1,p6"), new IntWritable(1)),
        new Pair<>(new Text("p1,p7"), new IntWritable(1)),
        new Pair<>(new Text("p1,p8"), new IntWritable(1)),
        new Pair<>(new Text("p2,p3"), new IntWritable(2)),
        new Pair<>(new Text("p2,p4"), new IntWritable(1)),
        new Pair<>(new Text("p2,p5"), new IntWritable(2)),
        new Pair<>(new Text("p2,p6"), new IntWritable(1)),
        new Pair<>(new Text("p3,p6"), new IntWritable(1)),
        new Pair<>(new Text("p3,p7"), new IntWritable(1)),
        new Pair<>(new Text("p3,p8"), new IntWritable(1)),
        new Pair<>(new Text("p4,p5"), new IntWritable(1)),
        new Pair<>(new Text("p7,p8"), new IntWritable(1))
    ));
    
    mapRedDriver.runTest();
  }
}
