package org.mdp.learn.hadoop.inverted_index;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class InvertedIndexTest {
  private MapDriver<LongWritable, Text, TermInfo, Posting>                       mapDriver;
  private ReduceDriver<TermInfo, Posting, Text, Postings>                        reducerDriver;
  private MapReduceDriver<LongWritable, Text, TermInfo, Posting, Text, Postings> mapRedDriver;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    IIMapper mapper = new IIMapper();
    mapDriver = MapDriver.newMapDriver(mapper);

    IIReducer reducer = new IIReducer();
    reducerDriver = ReduceDriver.newReduceDriver(reducer);

    mapRedDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    mapRedDriver.setKeyGroupingComparator(new SecondarySort.TermGroupingComparator());
  }

  @Test
  public void testMap() throws IOException {
    mapDriver.setMapInputPath(new Path("1"));

    mapDriver.withInput(new LongWritable(0), new Text("marco fabio luca"));
    mapDriver.withInput(new LongWritable(1), new Text("marco fabio luca"));
    mapDriver.withInput(new LongWritable(2), new Text("marco fabio luca"));
    mapDriver.withInput(new LongWritable(3), new Text("marco murray luca"));
    mapDriver.withInput(new LongWritable(4), new Text("marco murray gordon"));
    mapDriver.withInput(new LongWritable(3), new Text("marco murray gordon"));
    mapDriver.withInput(new LongWritable(4), new Text("marco murray richard"));

    mapDriver.withOutput(new TermInfo("marco", 1), new Posting(1, "7"));
    mapDriver.withOutput(new TermInfo("fabio", 1), new Posting(1, "3"));
    mapDriver.withOutput(new TermInfo("murray", 1), new Posting(1, "4"));
    mapDriver.withOutput(new TermInfo("luca", 1), new Posting(1, "4"));
    mapDriver.withOutput(new TermInfo("gordon", 1), new Posting(1, "2"));
    mapDriver.withOutput(new TermInfo("richard", 1), new Posting(1, "1"));

    mapDriver.runTest(false);
  }

  @Test
  public void testReduce() throws IOException {

    reducerDriver.withInput(new TermInfo("marco", 1), Arrays.asList(new Posting(1, "7"), new Posting(2, "5")));
    reducerDriver.withInput(new TermInfo("fabio", 1), Arrays.asList(new Posting(1, "3"), new Posting(2, "4")));
    reducerDriver.withInput(new TermInfo("murray", 1), Arrays.asList(new Posting(1, "4"), new Posting(4, "9"), new Posting(21, "4")));

    reducerDriver.withOutput(new Text("marco"), new Postings(new Posting[] { new Posting(1, "7"), new Posting(1, "5") }));
    reducerDriver.withOutput(new Text("fabio"), new Postings(new Posting[] { new Posting(1, "3"), new Posting(1, "4") }));
    reducerDriver.withOutput(new Text("murray"), new Postings(new Posting[] { new Posting(1, "4"), new Posting(3, "9"), new Posting(18, "4") }));

    reducerDriver.runTest();
  }

  @Test
  public void testMapReduce() throws IOException {
    mapRedDriver.setMapInputPath(new Path("1"));

    mapRedDriver.withInput(new LongWritable(0), new Text("Marco fabio luca"));
    mapRedDriver.withInput(new LongWritable(1), new Text("Marco fabio luca"));
    mapRedDriver.withInput(new LongWritable(2), new Text("Marco fabio luca"));
    mapRedDriver.withInput(new LongWritable(3), new Text("Marco murray luca"));
    mapRedDriver.withInput(new LongWritable(4), new Text("Marco murray gordon"));
    mapRedDriver.withInput(new LongWritable(3), new Text("Marco murray gordon"));
    mapRedDriver.withInput(new LongWritable(4), new Text("Marco murray richard"));

    mapRedDriver.run().forEach(System.out::println);
  }

}
