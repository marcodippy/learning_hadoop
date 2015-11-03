package org.mdp.learn.hadoop.dijkstra;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class DijkstraMapTest {
  private MapDriver<LongWritable, Text, Text, Text> mapDriver;

  @Before
  public void setUp() throws Exception {
    mapDriver = MapDriver.newMapDriver(new DijkstraMapper());
  }

  @Test
  public void testMap1() throws Exception {
    mapDriver.withInput(new LongWritable(0), new Text("MPX-[(JFK; 190),(HAV; 10)]-0"));
    mapDriver.withInput(new LongWritable(0), new Text("JFK-[(MIA; 60)]-INF"));
    mapDriver.withInput(new LongWritable(0), new Text("HAV-[(MIA; 10)]-INF"));
    mapDriver.withInput(new LongWritable(0), new Text("MIA-[(JFK; 10)]-INF"));

    mapDriver.withOutput(new Pair<Text, Text>(new Text("MPX"), new Text("VALUE=MPX-[(JFK; 190),(HAV; 10)]-0")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("JFK"), new Text("190-/MPX/JFK")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("HAV"), new Text("10-/MPX/HAV")));
    
    mapDriver.withOutput(new Pair<Text, Text>(new Text("JFK"), new Text("VALUE=JFK-[(MIA; 60)]-INF")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("MIA"), new Text("INF")));

    mapDriver.withOutput(new Pair<Text, Text>(new Text("HAV"), new Text("VALUE=HAV-[(MIA; 10)]-INF")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("MIA"), new Text("INF")));
  
    mapDriver.withOutput(new Pair<Text, Text>(new Text("MIA"), new Text("VALUE=MIA-[(JFK; 10)]-INF")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("JFK"), new Text("INF")));
    
    mapDriver.runTest();
  }
  
  @Test
  public void testMap2() throws Exception {
    mapDriver.withInput(new LongWritable(0), new Text("HAV-[(MIA; 10)]-10-/MPX/HAV"));
    mapDriver.withInput(new LongWritable(0), new Text("JFK-[(MIA; 60)]-190-/MPX/JFK"));
    mapDriver.withInput(new LongWritable(0), new Text("MIA-[(JFK; 10)]-INF"));
    mapDriver.withInput(new LongWritable(0), new Text("MPX-[(JFK; 190),(HAV; 10)]-0"));

    mapDriver.withOutput(new Pair<Text, Text>(new Text("HAV"), new Text("VALUE=HAV-[(MIA; 10)]-10-/MPX/HAV")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("MIA"), new Text("20-/MPX/HAV/MIA")));
    
    mapDriver.withOutput(new Pair<Text, Text>(new Text("JFK"), new Text("VALUE=JFK-[(MIA; 60)]-190-/MPX/JFK")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("MIA"), new Text("250-/MPX/JFK/MIA")));

    mapDriver.withOutput(new Pair<Text, Text>(new Text("MIA"), new Text("VALUE=MIA-[(JFK; 10)]-INF")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("JFK"), new Text("INF")));
    
    mapDriver.withOutput(new Pair<Text, Text>(new Text("MPX"), new Text("VALUE=MPX-[(JFK; 190),(HAV; 10)]-0")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("JFK"), new Text("190-/MPX/JFK")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("HAV"), new Text("10-/MPX/HAV")));
    
    mapDriver.runTest();
  }

  
  @Test
  public void testMap3() throws Exception {
    mapDriver.withInput(new LongWritable(0), new Text("HAV-[(MIA; 10)]-10-/MPX/HAV"));
    mapDriver.withInput(new LongWritable(0), new Text("JFK-[(MIA; 60)]-190-/MPX/JFK"));
    mapDriver.withInput(new LongWritable(0), new Text("MIA-[(JFK; 10)]-20-/MPX/HAV/MIA"));
    mapDriver.withInput(new LongWritable(0), new Text("MPX-[(JFK; 190),(HAV; 10)]-0"));

    mapDriver.withOutput(new Pair<Text, Text>(new Text("HAV"), new Text("VALUE=HAV-[(MIA; 10)]-10-/MPX/HAV")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("MIA"), new Text("20-/MPX/HAV/MIA")));
    
    mapDriver.withOutput(new Pair<Text, Text>(new Text("JFK"), new Text("VALUE=JFK-[(MIA; 60)]-190-/MPX/JFK")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("MIA"), new Text("250-/MPX/JFK/MIA")));

    mapDriver.withOutput(new Pair<Text, Text>(new Text("MIA"), new Text("VALUE=MIA-[(JFK; 10)]-20-/MPX/HAV/MIA")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("JFK"), new Text("30-/MPX/HAV/MIA/JFK")));
    
    mapDriver.withOutput(new Pair<Text, Text>(new Text("MPX"), new Text("VALUE=MPX-[(JFK; 190),(HAV; 10)]-0")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("JFK"), new Text("190-/MPX/JFK")));
    mapDriver.withOutput(new Pair<Text, Text>(new Text("HAV"), new Text("10-/MPX/HAV")));
    
    mapDriver.runTest();
  }
}
