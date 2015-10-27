package org.mdp.learn.hadoop.order_inversion.stripes;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class CoOccurrenceMatrixWithStripesTest {
  private MapDriver<LongWritable, Text, Text, MapWritable>                          mapDriver;
  private ReduceDriver<Text, MapWritable, Text, MapWritable>                        reducerDriver;
  private MapReduceDriver<LongWritable, Text, Text, MapWritable, Text, MapWritable> mapRedDriver;
  private final IntWritable                                                         ONE = new IntWritable(1);

  @Before
  public void setUp() throws Exception {
    CoOccurrenceMatrixMapperWithStripes mapper = new CoOccurrenceMatrixMapperWithStripes();
    mapDriver = MapDriver.newMapDriver(mapper);

    CoOccurrenceMatrixReducerWithStripes reducer = new CoOccurrenceMatrixReducerWithStripes();
    reducerDriver = ReduceDriver.newReduceDriver(reducer);

    mapRedDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new LongWritable(0), new Text("Test with a test sentence"));

    MapWritable map_test = new MapWritable();
    map_test.put(new Text("with"), ONE);
    map_test.put(new Text("a"), ONE);
    
    MapWritable map_with = new MapWritable();
    map_with.put(new Text("test"), new IntWritable(2));
    map_with.put(new Text("a"), ONE);
    
    MapWritable map_a = new MapWritable();
    map_a.put(new Text("test"), new IntWritable(2));
    map_a.put(new Text("with"), ONE);
    map_a.put(new Text("sentence"), ONE);
    
    MapWritable map_test2 = new MapWritable();
    map_test2.put(new Text("with"), ONE);
    map_test2.put(new Text("a"), ONE);
    map_test2.put(new Text("sentence"), ONE);
    
    MapWritable map_sentence = new MapWritable();
    map_sentence.put(new Text("a"), ONE);
    map_sentence.put(new Text("test"), ONE);
    
    mapDriver.withOutput(new Text("test"), map_test)
             .withOutput(new Text("with"), map_with)
             .withOutput(new Text("a"), map_a)
             .withOutput(new Text("test"), map_test2)
             .withOutput(new Text("sentence"), map_sentence);
    
    mapDriver.runTest(false);
  }
  
  @Test
  public void testReducer() throws IOException {
    MapWritable map_test = new MapWritable();
    map_test.put(new Text("with"), ONE);
    map_test.put(new Text("a"), ONE);
    
    MapWritable map_with = new MapWritable();
    map_with.put(new Text("test"), new IntWritable(2));
    map_with.put(new Text("a"), ONE);
    
    MapWritable map_a = new MapWritable();
    map_a.put(new Text("test"), new IntWritable(2));
    map_a.put(new Text("with"), ONE);
    map_a.put(new Text("sentence"), ONE);
    
    MapWritable map_test2 = new MapWritable();
    map_test2.put(new Text("with"), ONE);
    map_test2.put(new Text("a"), ONE);
    map_test2.put(new Text("sentence"), ONE);
    
    MapWritable map_sentence = new MapWritable();
    map_sentence.put(new Text("a"), ONE);
    map_sentence.put(new Text("test"), ONE);
    
    reducerDriver.withInput(new Text("test"), Arrays.asList(map_test, map_test2))
                 .withInput(new Text("with"), Arrays.asList(map_with))
                 .withInput(new Text("a"), Arrays.asList(map_a))
                 .withInput(new Text("sentence"), Arrays.asList(map_sentence));
  
    MapWritable map_test_final = new MapWritable();
    map_test_final.put(new Text("with"), new DoubleWritable(0.4));
    map_test_final.put(new Text("a"), new DoubleWritable(0.4));
    map_test_final.put(new Text("sentence"), new DoubleWritable(0.2));
    
    MapWritable map_with_final = new MapWritable();
    map_with_final.put(new Text("test"), new DoubleWritable(0.6666666666666666));
    map_with_final.put(new Text("a"), new DoubleWritable(0.3333333333333333));
    
    MapWritable map_a_final = new MapWritable();
    map_a_final.put(new Text("with"), new DoubleWritable(0.25));
    map_a_final.put(new Text("test"), new DoubleWritable(0.5));
    map_a_final.put(new Text("sentence"), new DoubleWritable(0.25));
    
    MapWritable map_sentence_final = new MapWritable();
    map_sentence_final.put(new Text("test"), new DoubleWritable(0.5));
    map_sentence_final.put(new Text("a"), new DoubleWritable(0.5));
    
    reducerDriver.withOutput(new Text("test"), map_test_final)
                 .withOutput(new Text("with"), map_with_final)
                 .withOutput(new Text("a"), map_a_final)
                 .withOutput(new Text("sentence"), map_sentence_final);
    
    reducerDriver.runTest();
  }

  @Test
  public void testMapReduce() throws IOException {
    mapRedDriver.withInput(new LongWritable(0), new Text("Test with a test sentence"));
    
    MapWritable map_test_final = new MapWritable();
    map_test_final.put(new Text("with"), new DoubleWritable(0.4));
    map_test_final.put(new Text("a"), new DoubleWritable(0.4));
    map_test_final.put(new Text("sentence"), new DoubleWritable(0.2));
    
    MapWritable map_with_final = new MapWritable();
    map_with_final.put(new Text("test"), new DoubleWritable(0.6666666666666666));
    map_with_final.put(new Text("a"), new DoubleWritable(0.3333333333333333));
    
    MapWritable map_a_final = new MapWritable();
    map_a_final.put(new Text("with"), new DoubleWritable(0.25));
    map_a_final.put(new Text("test"), new DoubleWritable(0.5));
    map_a_final.put(new Text("sentence"), new DoubleWritable(0.25));
    
    MapWritable map_sentence_final = new MapWritable();
    map_sentence_final.put(new Text("test"), new DoubleWritable(0.5));
    map_sentence_final.put(new Text("a"), new DoubleWritable(0.5));
    
    mapRedDriver.withOutput(new Text("test"), map_test_final)
                .withOutput(new Text("with"), map_with_final)
                .withOutput(new Text("a"), map_a_final)
                .withOutput(new Text("sentence"), map_sentence_final);
  }
}
