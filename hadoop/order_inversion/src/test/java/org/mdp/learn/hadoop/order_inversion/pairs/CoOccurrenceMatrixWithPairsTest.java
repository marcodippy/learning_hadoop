package org.mdp.learn.hadoop.order_inversion.pairs;

import java.io.IOException;

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

  /*
  @Test
  public void testReducer() throws IOException {
    List<IntWritable> oneList = Arrays.asList(ONE);

    reducerDriver.withInput(new TextPair("this", "is"), oneList).withInput(new TextPair("this", "a"), oneList).withInput(new TextPair("this", "test"), oneList)
        .withInput(new TextPair("this", "sentence"), Arrays.asList(ONE, ONE)).withInput(new TextPair("this", "contains"), oneList)
        .withInput(new TextPair("is", "this"), oneList).withInput(new TextPair("is", "a"), oneList).withInput(new TextPair("is", "test"), oneList)
        .withInput(new TextPair("a", "this"), oneList).withInput(new TextPair("a", "is"), oneList).withInput(new TextPair("a", "test"), oneList)
        .withInput(new TextPair("a", "sentence"), oneList).withInput(new TextPair("test", "is"), oneList).withInput(new TextPair("test", "a"), oneList)
        .withInput(new TextPair("test", "sentence"), oneList).withInput(new TextPair("test", "this"), oneList).withInput(new TextPair("sentence", "a"), oneList)
        .withInput(new TextPair("sentence", "test"), oneList).withInput(new TextPair("sentence", "this"), Arrays.asList(ONE, ONE))
        .withInput(new TextPair("sentence", "sentence"), Arrays.asList(ONE, ONE)).withInput(new TextPair("sentence", "contains"), oneList)
        .withInput(new TextPair("sentence", "words"), oneList).withInput(new TextPair("contains", "this"), oneList)
        .withInput(new TextPair("contains", "sentence"), oneList).withInput(new TextPair("contains", "words"), oneList)
        .withInput(new TextPair("words", "sentence"), oneList).withInput(new TextPair("words", "contains"), oneList);

    reducerDriver.withOutput(new TextPair("this", "is"), ONE).withOutput(new TextPair("this", "a"), ONE).withOutput(new TextPair("this", "test"), ONE)
        .withOutput(new TextPair("this", "sentence"), new IntWritable(2)).withOutput(new TextPair("this", "contains"), ONE)
        .withOutput(new TextPair("is", "this"), ONE).withOutput(new TextPair("is", "a"), ONE).withOutput(new TextPair("is", "test"), ONE)
        .withOutput(new TextPair("a", "this"), ONE).withOutput(new TextPair("a", "is"), ONE).withOutput(new TextPair("a", "test"), ONE)
        .withOutput(new TextPair("a", "sentence"), ONE).withOutput(new TextPair("test", "is"), ONE).withOutput(new TextPair("test", "a"), ONE)
        .withOutput(new TextPair("test", "sentence"), ONE).withOutput(new TextPair("test", "this"), ONE).withOutput(new TextPair("sentence", "a"), ONE)
        .withOutput(new TextPair("sentence", "test"), ONE).withOutput(new TextPair("sentence", "sentence"), new IntWritable(2))
        .withOutput(new TextPair("sentence", "this"), new IntWritable(2)).withOutput(new TextPair("sentence", "contains"), ONE)
        .withOutput(new TextPair("sentence", "words"), ONE).withOutput(new TextPair("contains", "this"), ONE)
        .withOutput(new TextPair("contains", "sentence"), ONE).withOutput(new TextPair("contains", "words"), ONE)
        .withOutput(new TextPair("words", "sentence"), ONE).withOutput(new TextPair("words", "contains"), ONE);

    reducerDriver.runTest(false);
  }

  @Test
  public void testMapReduce() throws IOException {
    mapRedDriver.withInput(new LongWritable(0), new Text("This is a test sentence, this sentence contains words"));

    mapRedDriver.withOutput(new TextPair("this", "is"), ONE).withOutput(new TextPair("this", "a"), ONE).withOutput(new TextPair("this", "test"), ONE)
        .withOutput(new TextPair("this", "sentence"), new IntWritable(2)).withOutput(new TextPair("this", "contains"), ONE)
        .withOutput(new TextPair("is", "this"), ONE).withOutput(new TextPair("is", "a"), ONE).withOutput(new TextPair("is", "test"), ONE)
        .withOutput(new TextPair("a", "this"), ONE).withOutput(new TextPair("a", "is"), ONE).withOutput(new TextPair("a", "test"), ONE)
        .withOutput(new TextPair("a", "sentence"), ONE).withOutput(new TextPair("test", "is"), ONE).withOutput(new TextPair("test", "a"), ONE)
        .withOutput(new TextPair("test", "sentence"), ONE).withOutput(new TextPair("test", "this"), ONE).withOutput(new TextPair("sentence", "a"), ONE)
        .withOutput(new TextPair("sentence", "test"), ONE).withOutput(new TextPair("sentence", "sentence"), new IntWritable(2))
        .withOutput(new TextPair("sentence", "this"), new IntWritable(2)).withOutput(new TextPair("sentence", "contains"), ONE)
        .withOutput(new TextPair("sentence", "words"), ONE).withOutput(new TextPair("contains", "this"), ONE)
        .withOutput(new TextPair("contains", "sentence"), ONE).withOutput(new TextPair("contains", "words"), ONE)
        .withOutput(new TextPair("words", "sentence"), ONE).withOutput(new TextPair("words", "contains"), ONE);

    mapRedDriver.runTest(false);
  }
  */
}
