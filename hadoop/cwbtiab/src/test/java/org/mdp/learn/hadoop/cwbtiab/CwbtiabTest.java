package org.mdp.learn.hadoop.cwbtiab;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class CwbtiabTest {
  private MapDriver<LongWritable, Text, Text, Text>                                 mapDriver;
  private ReduceDriver<Text, Text, NullWritable, Text>                              reducerDriver;
  private MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text>       groupProductsMrDriver;
  private MapReduceDriver<LongWritable, Text, Text, MapWritable, Text, MapWritable> topNMrDriver;

  @Before
  public void setUp() throws Exception {
    GroupProductsMapper mapper = new GroupProductsMapper();
    mapDriver = MapDriver.newMapDriver(mapper);

    GroupProductsReducer reducer = new GroupProductsReducer();
    reducerDriver = ReduceDriver.newReduceDriver(reducer);

    groupProductsMrDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    topNMrDriver = MapReduceDriver.newMapReduceDriver(new TopNMapper(), new TopNReducer());
  }

  @Test
  public void test() throws IOException {
    groupProductsMrDriver.addAll(Arrays.asList(
        new Pair<>(new LongWritable(), new Text("u1,p1")),
        new Pair<>(new LongWritable(), new Text("u1,p2")),
        new Pair<>(new LongWritable(), new Text("u1,p3")),
        new Pair<>(new LongWritable(), new Text("u2,p1")),
        new Pair<>(new LongWritable(), new Text("u2,p2")),
        new Pair<>(new LongWritable(), new Text("u2,p5")),
        new Pair<>(new LongWritable(), new Text("u2,p6")),
        new Pair<>(new LongWritable(), new Text("u3,p1")),
        new Pair<>(new LongWritable(), new Text("u3,p6")),
        new Pair<>(new LongWritable(), new Text("u3,p8"))
    ));
    
    List<Pair<NullWritable, Text>> ret = groupProductsMrDriver.run();
    ret.forEach(System.out::println);
    
    List<Pair<LongWritable, Text>> inputs = ret.stream().map(pair -> new Pair<LongWritable, Text>(new LongWritable(), pair.getSecond())).collect(Collectors.toList());
    topNMrDriver.addAll(inputs);
    
    List<Pair<Text, MapWritable>> resultTopN = topNMrDriver.run();
    resultTopN.forEach(System.out::println);
    
  }

}
