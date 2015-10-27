package org.mdp.learn.hadoop.order_inversion.pairs;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.mdp.learn.hadoop.commons.TextPair;

public class CoOccurrenceMatrixWithPairsTest {
  private MapDriver<LongWritable, Text, TextPair, IntWritable>                                 mapDriver;
  private ReduceDriver<TextPair, IntWritable, TextPair, DoubleWritable>                        reducerDriver;
  private MapReduceDriver<LongWritable, Text, TextPair, IntWritable, TextPair, DoubleWritable> mapRedDriver;
  private final IntWritable                                                                    ONE = new IntWritable(1);

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    CoOccurrenceMatrixMapperWithPairs mapper = new CoOccurrenceMatrixMapperWithPairs();
    mapDriver = MapDriver.newMapDriver(mapper);

    CoOccurrenceMatrixReducerWithPairs reducer = new CoOccurrenceMatrixReducerWithPairs();
    reducerDriver = ReduceDriver.newReduceDriver(reducer);

    mapRedDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    mapRedDriver.setKeyComparator(new KeyComparator());
  }

  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new LongWritable(0), new Text("This is a test sentence, this sentence contains words"));

    mapDriver.withOutput(new TextPair("this", "is"), ONE)
             .withOutput(new TextPair("this", "a"), ONE)
             .withOutput(new TextPair("this", "*"), new IntWritable(2))
             .withOutput(new TextPair("is", "this"), ONE)
             .withOutput(new TextPair("is", "a"), ONE)
             .withOutput(new TextPair("is", "test"), ONE)
             .withOutput(new TextPair("is", "*"), new IntWritable(3))
             .withOutput(new TextPair("a", "this"), ONE)
             .withOutput(new TextPair("a", "is"), ONE)
             .withOutput(new TextPair("a", "test"), ONE)
             .withOutput(new TextPair("a", "sentence"), ONE)
             .withOutput(new TextPair("a", "*"), new IntWritable(4))
             .withOutput(new TextPair("test", "is"), ONE)
             .withOutput(new TextPair("test", "a"), ONE)
             .withOutput(new TextPair("test", "sentence"), ONE)
             .withOutput(new TextPair("test", "this"), ONE)
             .withOutput(new TextPair("test", "*"), new IntWritable(4))
             .withOutput(new TextPair("sentence", "a"), ONE)
             .withOutput(new TextPair("sentence", "test"), ONE)
             .withOutput(new TextPair("sentence", "this"), ONE)
             .withOutput(new TextPair("sentence", "sentence"), ONE)
             .withOutput(new TextPair("sentence", "*"), new IntWritable(4))
             .withOutput(new TextPair("this", "test"), ONE)
             .withOutput(new TextPair("this", "sentence"), ONE)
             .withOutput(new TextPair("this", "sentence"), ONE)
             .withOutput(new TextPair("this", "contains"), ONE)
             .withOutput(new TextPair("this", "*"), new IntWritable(4))
             .withOutput(new TextPair("sentence", "sentence"), ONE)
             .withOutput(new TextPair("sentence", "this"), ONE)
             .withOutput(new TextPair("sentence", "contains"), ONE)
             .withOutput(new TextPair("sentence", "words"), ONE)
             .withOutput(new TextPair("sentence", "*"), new IntWritable(4))
             .withOutput(new TextPair("contains", "this"), ONE)
             .withOutput(new TextPair("contains", "sentence"), ONE)
             .withOutput(new TextPair("contains", "words"), ONE)
             .withOutput(new TextPair("contains", "*"), new IntWritable(3))
             .withOutput(new TextPair("words", "sentence"), ONE)
             .withOutput(new TextPair("words", "contains"), ONE)
             .withOutput(new TextPair("words", "*"), new IntWritable(2));

    mapDriver.runTest(false);
  }

  @Test
  public void testReducer() throws IOException {
    List<IntWritable> ONE_LIST = Arrays.asList(ONE);

    reducerDriver
    .withInput(new TextPair("a", "*"), Arrays.asList(new IntWritable(4)))
      .withInput(new TextPair("a", "this"), ONE_LIST)
      .withInput(new TextPair("a", "is"), ONE_LIST)
      .withInput(new TextPair("a", "test"), ONE_LIST)
      .withInput(new TextPair("a", "sentence"), ONE_LIST)
      
    .withInput(new TextPair("contains", "*"), Arrays.asList(new IntWritable(3)))  
      .withInput(new TextPair("contains", "this"), ONE_LIST)
      .withInput(new TextPair("contains", "sentence"), ONE_LIST)
      .withInput(new TextPair("contains", "words"), ONE_LIST)
      
    .withInput(new TextPair("is", "*"), Arrays.asList(new IntWritable(3)))
      .withInput(new TextPair("is", "this"), ONE_LIST)
      .withInput(new TextPair("is", "a"), ONE_LIST)
      .withInput(new TextPair("is", "test"), ONE_LIST)
      
    .withInput(new TextPair("sentence", "*"), Arrays.asList(new IntWritable(4)))
    .withInput(new TextPair("sentence", "*"), Arrays.asList(new IntWritable(4)))    
      .withInput(new TextPair("sentence", "a"), ONE_LIST)
      .withInput(new TextPair("sentence", "test"), ONE_LIST)
      .withInput(new TextPair("sentence", "this"), Arrays.asList(ONE, ONE))
      .withInput(new TextPair("sentence", "sentence"), Arrays.asList(ONE, ONE))
      .withInput(new TextPair("sentence", "contains"), ONE_LIST)
      .withInput(new TextPair("sentence", "words"), ONE_LIST)
      
    .withInput(new TextPair("test", "*"), Arrays.asList(new IntWritable(4)))
      .withInput(new TextPair("test", "is"), ONE_LIST)
      .withInput(new TextPair("test", "a"), ONE_LIST)
      .withInput(new TextPair("test", "sentence"), ONE_LIST)
      .withInput(new TextPair("test", "this"), ONE_LIST)
      
    .withInput(new TextPair("this", "*"), Arrays.asList(new IntWritable(2)))
    .withInput(new TextPair("this", "*"), Arrays.asList(new IntWritable(4)))
      .withInput(new TextPair("this", "is"), ONE_LIST)
      .withInput(new TextPair("this", "a"), ONE_LIST)
      .withInput(new TextPair("this", "test"), ONE_LIST)
      .withInput(new TextPair("this", "sentence"), Arrays.asList(ONE, ONE))
      .withInput(new TextPair("this", "contains"), ONE_LIST)
      
    .withInput(new TextPair("words", "*"), Arrays.asList(new IntWritable(2)))
      .withInput(new TextPair("words", "sentence"), ONE_LIST)
      .withInput(new TextPair("words", "contains"), ONE_LIST);
    
    reducerDriver
    .withOutput(new TextPair("a", "this"), new DoubleWritable(0.25))
    .withOutput(new TextPair("a", "is"), new DoubleWritable(0.25))
    .withOutput(new TextPair("a", "test"), new DoubleWritable(0.25))
    .withOutput(new TextPair("a", "sentence"), new DoubleWritable(0.25))
    .withOutput(new TextPair("contains", "this"), new DoubleWritable(0.3333333333333333))
    .withOutput(new TextPair("contains", "sentence"), new DoubleWritable(0.3333333333333333))
    .withOutput(new TextPair("contains", "words"), new DoubleWritable(0.3333333333333333))
    .withOutput(new TextPair("is", "this"), new DoubleWritable(0.3333333333333333))
    .withOutput(new TextPair("is", "a"), new DoubleWritable(0.3333333333333333))
    .withOutput(new TextPair("is", "test"), new DoubleWritable(0.3333333333333333))
    .withOutput(new TextPair("sentence", "a"), new DoubleWritable(0.125))
    .withOutput(new TextPair("sentence", "test"), new DoubleWritable(0.125))
    .withOutput(new TextPair("sentence", "this"), new DoubleWritable(0.25))
    .withOutput(new TextPair("sentence", "sentence"), new DoubleWritable(0.25))
    .withOutput(new TextPair("sentence", "contains"), new DoubleWritable(0.125))
    .withOutput(new TextPair("sentence", "words"), new DoubleWritable(0.125))
    .withOutput(new TextPair("test", "is"), new DoubleWritable(0.25))
    .withOutput(new TextPair("test", "a"), new DoubleWritable(0.25))
    .withOutput(new TextPair("test", "sentence"), new DoubleWritable(0.25))
    .withOutput(new TextPair("test", "this"), new DoubleWritable(0.25))
    .withOutput(new TextPair("this", "is"), new DoubleWritable(0.16666666666666666))
    .withOutput(new TextPair("this", "a"), new DoubleWritable(0.16666666666666666))
    .withOutput(new TextPair("this", "test"), new DoubleWritable(0.16666666666666666))
    .withOutput(new TextPair("this", "sentence"), new DoubleWritable(0.3333333333333333))
    .withOutput(new TextPair("this", "contains"), new DoubleWritable(0.16666666666666666))
    .withOutput(new TextPair("words", "sentence"), new DoubleWritable(0.5))
    .withOutput(new TextPair("words", "contains"), new DoubleWritable(0.5));
    
    reducerDriver.runTest();
  }

  @Test
  public void testMapReduce() throws IOException {
    mapRedDriver.withInput(new LongWritable(0), new Text("This is a test sentence, this sentence contains words"));

    mapRedDriver
      .withOutput(new TextPair("a", "this"), new DoubleWritable(0.25))
      .withOutput(new TextPair("a", "is"), new DoubleWritable(0.25))
      .withOutput(new TextPair("a", "test"), new DoubleWritable(0.25))
      .withOutput(new TextPair("a", "sentence"), new DoubleWritable(0.25))
      .withOutput(new TextPair("contains", "this"), new DoubleWritable(0.3333333333333333))
      .withOutput(new TextPair("contains", "sentence"), new DoubleWritable(0.3333333333333333))
      .withOutput(new TextPair("contains", "words"), new DoubleWritable(0.3333333333333333))
      .withOutput(new TextPair("is", "this"), new DoubleWritable(0.3333333333333333))
      .withOutput(new TextPair("is", "a"), new DoubleWritable(0.3333333333333333))
      .withOutput(new TextPair("is", "test"), new DoubleWritable(0.3333333333333333))
      .withOutput(new TextPair("sentence", "a"), new DoubleWritable(0.125))
      .withOutput(new TextPair("sentence", "test"), new DoubleWritable(0.125))
      .withOutput(new TextPair("sentence", "this"), new DoubleWritable(0.25))
      .withOutput(new TextPair("sentence", "sentence"), new DoubleWritable(0.25))
      .withOutput(new TextPair("sentence", "contains"), new DoubleWritable(0.125))
      .withOutput(new TextPair("sentence", "words"), new DoubleWritable(0.125))
      .withOutput(new TextPair("test", "is"), new DoubleWritable(0.25))
      .withOutput(new TextPair("test", "a"), new DoubleWritable(0.25))
      .withOutput(new TextPair("test", "sentence"), new DoubleWritable(0.25))
      .withOutput(new TextPair("test", "this"), new DoubleWritable(0.25))
      .withOutput(new TextPair("this", "is"), new DoubleWritable(0.16666666666666666))
      .withOutput(new TextPair("this", "a"), new DoubleWritable(0.16666666666666666))
      .withOutput(new TextPair("this", "test"), new DoubleWritable(0.16666666666666666))
      .withOutput(new TextPair("this", "sentence"), new DoubleWritable(0.3333333333333333))
      .withOutput(new TextPair("this", "contains"), new DoubleWritable(0.16666666666666666))
      .withOutput(new TextPair("words", "sentence"), new DoubleWritable(0.5))
      .withOutput(new TextPair("words", "contains"), new DoubleWritable(0.5));

    mapRedDriver.runTest(false);
  }
}
