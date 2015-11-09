package org.mdp.learn.hadoop.people_you_may_know;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mdp.learn.hadoop.commons.TextPair;

public class PymkTest {
  private MapDriver<LongWritable, Text, Text, TextPair>                   mapDriver;
  private ReduceDriver<Text, TextPair, Text, Text>                        reducerDriver;
  private MapReduceDriver<LongWritable, Text, Text, TextPair, Text, Text> mapRedDriver;

  @Before
  public void setUp() throws Exception {
    PymkMapper mapper = new PymkMapper();
    mapDriver = MapDriver.newMapDriver(mapper);

    PymkReducer reducer = new PymkReducer();
    reducerDriver = ReduceDriver.newReduceDriver(reducer);

    mapRedDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMap() throws IOException {
    mapDriver.withInput(new LongWritable(1), new Text("Marco-Fabio,Luca,Gordon"));
    mapDriver.withInput(new LongWritable(2), new Text("Fabio-Nathalia,Marco"));
    mapDriver.withInput(new LongWritable(3), new Text("Luca-Nathalia,Marco"));
    mapDriver.withInput(new LongWritable(4), new Text("Nathalia-Fabio,Luca"));
    mapDriver.withInput(new LongWritable(5), new Text("Gordon-Marco"));
   
    mapDriver.withOutput(new Text("Marco"), new TextPair("Fabio","FRIENDS"));
    mapDriver.withOutput(new Text("Marco"), new TextPair("Luca","FRIENDS"));
    mapDriver.withOutput(new Text("Marco"), new TextPair("Gordon","FRIENDS"));
    mapDriver.withOutput(new Text("Luca"), new TextPair("Fabio","Marco"));
    mapDriver.withOutput(new Text("Fabio"), new TextPair("Luca","Marco"));
    mapDriver.withOutput(new Text("Gordon"), new TextPair("Fabio","Marco"));
    mapDriver.withOutput(new Text("Fabio"), new TextPair("Gordon","Marco"));
    mapDriver.withOutput(new Text("Gordon"), new TextPair("Luca","Marco"));
    mapDriver.withOutput(new Text("Luca"), new TextPair("Gordon","Marco"));
    mapDriver.withOutput(new Text("Fabio"), new TextPair("Nathalia","FRIENDS"));
    mapDriver.withOutput(new Text("Fabio"), new TextPair("Marco","FRIENDS"));
    mapDriver.withOutput(new Text("Marco"), new TextPair("Nathalia","Fabio"));
    mapDriver.withOutput(new Text("Nathalia"), new TextPair("Marco","Fabio"));
    mapDriver.withOutput(new Text("Luca"), new TextPair("Nathalia","FRIENDS"));
    mapDriver.withOutput(new Text("Luca"), new TextPair("Marco","FRIENDS"));
    mapDriver.withOutput(new Text("Marco"), new TextPair("Nathalia","Luca"));
    mapDriver.withOutput(new Text("Nathalia"), new TextPair("Marco","Luca"));
    mapDriver.withOutput(new Text("Nathalia"),new TextPair("Fabio","FRIENDS"));
    mapDriver.withOutput(new Text("Nathalia"), new TextPair("Luca","FRIENDS"));
    mapDriver.withOutput(new Text("Luca"), new TextPair("Fabio", "Nathalia"));
    mapDriver.withOutput(new Text("Fabio"), new TextPair("Luca","Nathalia"));
    mapDriver.withOutput(new Text("Gordon"), new TextPair("Marco","FRIENDS"));

    mapDriver.runTest();
  }
  
  @Test
  public void testReducer() throws IOException {
    reducerDriver.withInput(new Text("Marco"), Arrays.asList(new TextPair("Fabio","FRIENDS"),
                                                             new TextPair("Luca","FRIENDS"),
                                                             new TextPair("Gordon","FRIENDS"),
                                                             new TextPair("Nathalia","Fabio"),
                                                             new TextPair("Nathalia","Luca")));
    
    reducerDriver.withInput(new Text("Gordon"), Arrays.asList(new TextPair("Fabio","Marco"), 
                                                              new TextPair("Marco","FRIENDS"), 
                                                              new TextPair("Luca","Marco")));
    
    reducerDriver.withInput(new Text("Luca"), Arrays.asList(new TextPair("Fabio", "Nathalia"),
                                                            new TextPair("Fabio","Marco"),
                                                            new TextPair("Gordon","Marco"),
                                                            new TextPair("Nathalia","FRIENDS"),
                                                            new TextPair("Marco","FRIENDS")));

    reducerDriver.withInput(new Text("Fabio"), Arrays.asList(new TextPair("Luca","Marco"),
                                                             new TextPair("Luca","Nathalia"),
                                                             new TextPair("Gordon","Marco"),
                                                             new TextPair("Nathalia","FRIENDS"),
                                                             new TextPair("Marco","FRIENDS")));
    
    reducerDriver.withInput(new Text("Nathalia"), Arrays.asList(new TextPair("Marco","Fabio"),
                                                                new TextPair("Marco","Luca"),
                                                                new TextPair("Luca","FRIENDS"),
                                                                new TextPair("Fabio","FRIENDS")));
    
    reducerDriver.withAllOutput(Arrays.asList(
        new Pair<>(new Text("Gordon"), new Text("Luca(1:[Marco]),Fabio(1:[Marco])")),
        new Pair<>(new Text("Luca"), new Text("Gordon(1:[Marco]),Fabio(2:[Nathalia, Marco])")), 
        new Pair<>(new Text("Marco"), new Text("Nathalia(2:[Fabio, Luca])")),
        new Pair<>(new Text("Fabio"), new Text("Gordon(1:[Marco]),Luca(2:[Marco, Nathalia])")),
        new Pair<>(new Text("Nathalia"), new Text("Marco(2:[Fabio, Luca])"))));
    
    reducerDriver.runTest(false);
  }
  
  @Test
  public void testMapReduce() throws IOException {
    mapRedDriver.withInput(new LongWritable(1), new Text("Marco-Fabio,Luca,Gordon"));
    mapRedDriver.withInput(new LongWritable(2), new Text("Fabio-Nathalia,Marco,Luca,Gordon"));
    mapRedDriver.withInput(new LongWritable(3), new Text("Luca-Nathalia,Marco,Fabio"));
    mapRedDriver.withInput(new LongWritable(4), new Text("Nathalia-Fabio,Luca"));
    mapRedDriver.withInput(new LongWritable(5), new Text("Gordon-Marco,Fabio"));

    mapRedDriver.withAllOutput(Arrays.asList(
        new Pair<>(new Text("Gordon"), new Text("Luca(2:[Marco, Fabio]),Nathalia(1:[Fabio])")),
        new Pair<>(new Text("Luca"), new Text("Gordon(2:[Marco, Fabio])")), 
        new Pair<>(new Text("Marco"), new Text("Nathalia(2:[Fabio, Luca])")),
        new Pair<>(new Text("Nathalia"), new Text("Marco(2:[Fabio, Luca]),Gordon(1:[Fabio])"))));

    mapRedDriver.runTest();
  }
}
