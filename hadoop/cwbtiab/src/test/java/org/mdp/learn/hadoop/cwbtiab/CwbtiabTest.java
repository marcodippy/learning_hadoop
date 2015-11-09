package org.mdp.learn.hadoop.cwbtiab;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class CwbtiabTest {
  private MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text>       groupProductsMrDriver;
  private MapReduceDriver<LongWritable, Text, Text, MapWritable, NullWritable, Text> topNMrDriver;

  @Before
  public void setUp() throws Exception {
    groupProductsMrDriver = MapReduceDriver.newMapReduceDriver(new GroupProductsMapper(), new GroupProductsReducer());
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
        new Pair<>(new LongWritable(), new Text("u3,p8")),
        new Pair<>(new LongWritable(), new Text("u4,p1")),
        new Pair<>(new LongWritable(), new Text("u4,p5")),
        new Pair<>(new LongWritable(), new Text("u4,p3")),
        new Pair<>(new LongWritable(), new Text("u4,p6")),
        new Pair<>(new LongWritable(), new Text("u5,p1")),
        new Pair<>(new LongWritable(), new Text("u3,p5"))
    ));
    
    List<Pair<NullWritable, Text>> ret = groupProductsMrDriver.run();
    ret.forEach(System.out::println);
    
    List<Pair<LongWritable, Text>> inputs = ret.stream().map(pair -> new Pair<LongWritable, Text>(new LongWritable(), pair.getSecond())).collect(Collectors.toList());
    topNMrDriver.addAll(inputs);
    
    List<Pair<NullWritable, Text>> resultTopN = topNMrDriver.run();
    resultTopN.forEach(System.out::println);
    
    assertThat(resultTopN).containsAll(
        Arrays.asList(
            new Pair<>(NullWritable.get(), new Text("p1-(p3,2),(p6,3),(p5,3)")),
            new Pair<>(NullWritable.get(), new Text("p2-(p6,1),(p1,2),(p5,1)")),
            new Pair<>(NullWritable.get(), new Text("p3-(p6,1),(p1,2),(p5,1)")),
            new Pair<>(NullWritable.get(), new Text("p5-(p8,1),(p1,3),(p6,3)")),
            new Pair<>(NullWritable.get(), new Text("p6-(p8,1),(p1,3),(p5,3)")),
            new Pair<>(NullWritable.get(), new Text("p8-(p1,1),(p5,1),(p6,1)"))
    ));
  }

}
