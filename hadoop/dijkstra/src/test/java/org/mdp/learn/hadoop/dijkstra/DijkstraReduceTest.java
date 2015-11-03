package org.mdp.learn.hadoop.dijkstra;

import java.util.Arrays;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class DijkstraReduceTest {
  private ReduceDriver<Text, Text, NullWritable, Text> redDriver;

  @Before
  public void setUp() throws Exception {
    redDriver = ReduceDriver.newReduceDriver(new DijkstraReducer());
  }

  @Test
  public void testRed1() throws Exception {
    redDriver.addInput(new Text("MPX"), Arrays.asList(new Text("VALUE=MPX-[(JFK; 190),(HAV; 10)]-0")));
    redDriver.addInput(new Text("JFK"), Arrays.asList(new Text("190-/MPX/JFK"), new Text("VALUE=JFK-[(MIA; 60)]-INF"),  new Text("INF")));
    redDriver.addInput(new Text("HAV"), Arrays.asList(new Text("VALUE=HAV-[(MIA; 10)]-INF"), new Text("10-/MPX/HAV")));
    redDriver.addInput(new Text("MIA"), Arrays.asList(new Text("VALUE=MIA-[(JFK; 10)]-INF"), new Text("INF"), new Text("INF")));

    redDriver.withOutput(NullWritable.get(), new Text("MPX-[(JFK; 190),(HAV; 10)]-0"));
    redDriver.withOutput(NullWritable.get(), new Text("JFK-[(MIA; 60)]-190-/MPX/JFK"));
    redDriver.withOutput(NullWritable.get(), new Text("HAV-[(MIA; 10)]-10-/MPX/HAV"));
    redDriver.withOutput(NullWritable.get(), new Text("MIA-[(JFK; 10)]-INF"));

    redDriver.runTest();
  }
  
  @Test
  public void testRed2() throws Exception {
    redDriver.addInput(new Text("MPX"), Arrays.asList(new Text("VALUE=MPX-[(JFK; 190),(HAV; 10)]-0")));
    redDriver.addInput(new Text("JFK"), Arrays.asList(new Text("VALUE=JFK-[(MIA; 60)]-190-/MPX/JFK"), new Text("190-/MPX/JFK"),  new Text("INF")));
    redDriver.addInput(new Text("HAV"), Arrays.asList(new Text("VALUE=HAV-[(MIA; 10)]-10-/MPX/HAV"), new Text("10-/MPX/HAV")));
    redDriver.addInput(new Text("MIA"), Arrays.asList(new Text("VALUE=MIA-[(JFK; 10)]-INF"), new Text("20-/MPX/HAV/MIA"), new Text("250-/MPX/JFK/MIA")));

    redDriver.withOutput(NullWritable.get(), new Text("MPX-[(JFK; 190),(HAV; 10)]-0"));
    redDriver.withOutput(NullWritable.get(), new Text("JFK-[(MIA; 60)]-190-/MPX/JFK"));
    redDriver.withOutput(NullWritable.get(), new Text("HAV-[(MIA; 10)]-10-/MPX/HAV"));
    redDriver.withOutput(NullWritable.get(), new Text("MIA-[(JFK; 10)]-20-/MPX/HAV/MIA"));

    redDriver.runTest();
  }
  
  @Test
  public void testRed3() throws Exception {
    redDriver.addInput(new Text("MPX"), Arrays.asList(new Text("VALUE=MPX-[(JFK; 190),(HAV; 10)]-0")));
    redDriver.addInput(new Text("JFK"), Arrays.asList(new Text("VALUE=JFK-[(MIA; 60)]-190-/MPX/JFK"), new Text("190-/MPX/JFK"),  new Text("30-/MPX/HAV/MIA/JFK")));
    redDriver.addInput(new Text("HAV"), Arrays.asList(new Text("VALUE=HAV-[(MIA; 10)]-10-/MPX/HAV"), new Text("10-/MPX/HAV")));
    redDriver.addInput(new Text("MIA"), Arrays.asList(new Text("VALUE=MIA-[(JFK; 10)]-20-/MPX/HAV/MIA"), new Text("20-/MPX/HAV/MIA"), new Text("250-/MPX/JFK/MIA")));

    redDriver.withOutput(NullWritable.get(), new Text("MPX-[(JFK; 190),(HAV; 10)]-0"));
    redDriver.withOutput(NullWritable.get(), new Text("JFK-[(MIA; 60)]-30-/MPX/HAV/MIA/JFK"));
    redDriver.withOutput(NullWritable.get(), new Text("HAV-[(MIA; 10)]-10-/MPX/HAV"));
    redDriver.withOutput(NullWritable.get(), new Text("MIA-[(JFK; 10)]-20-/MPX/HAV/MIA"));

    redDriver.runTest();
  }
}
