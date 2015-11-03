package org.mdp.learn.hadoop.dijkstra;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class DijkstraMapReduceTest {
  private MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> mapRedDriver;

  @Before
  public void setUp() throws Exception {
    mapRedDriver = MapReduceDriver.newMapReduceDriver(new DijkstraMapper(), new DijkstraReducer());
    mapRedDriver.withCounters(new Counters());
  }

  /*
  ((null), HAV-[(MIA; 10)]-10-/MPX/HAV)
  ((null), JFK-[(MIA; 60)]-190-/MPX/JFK)
  ((null), MIA-[(JFK; 10)]-INF)
  ((null), MPX-[(JFK; 190),(HAV; 10)]-0)
  ******************
  ((null), HAV-[(MIA; 10)]-10-/MPX/HAV)
  ((null), JFK-[(MIA; 60)]-190-/MPX/JFK)
  ((null), MIA-[(JFK; 10)]-20-/MPX/HAV/MIA)
  ((null), MPX-[(JFK; 190),(HAV; 10)]-0)
  ******************
  ((null), HAV-[(MIA; 10)]-10-/MPX/HAV)
  ((null), JFK-[(MIA; 60)]-30-/MPX/HAV/MIA/JFK)
  ((null), MIA-[(JFK; 10)]-20-/MPX/HAV/MIA)
  ((null), MPX-[(JFK; 190),(HAV; 10)]-0)
  */
  
  @SuppressWarnings("unchecked")
  @Test
  public void testJob() throws Exception {
    mapRedDriver.withInput(new LongWritable(0), new Text("MPX-[(JFK; 190),(HAV; 10)]-0"));
    mapRedDriver.withInput(new LongWritable(0), new Text("JFK-[(MIA; 60)]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("HAV-[(MIA; 10)]-INF"));
    mapRedDriver.withInput(new LongWritable(0), new Text("MIA-[(JFK; 10)]-INF"));

    int cnt = 1;

    List<Pair<NullWritable, Text>> ret = mapRedDriver.run();
    long changedNodes = mapRedDriver.getCounters().findCounter(DijkstraCounters.CHANGED_NODES).getValue();

    ret.forEach(System.out::println);
    System.out.println("******************");

    cnt++;
    
    do {
      setUp();
      List<Pair<LongWritable, Text>> inputs = ret.stream().map(x -> new Pair<LongWritable, Text>(new LongWritable(0), x.getSecond())).collect(Collectors.toList());
      ret = mapRedDriver.withAll(inputs).run();
      changedNodes = mapRedDriver.getCounters().findCounter(DijkstraCounters.CHANGED_NODES).getValue();
      
      ret.forEach(System.out::println);
      System.out.println("******************");
      
      cnt++;
    }
    while (changedNodes > 0);

    assertThat(ret).containsExactly(
        new Pair<>(NullWritable.get(), new Text("HAV-[(MIA; 10)]-10-/MPX/HAV")),
        new Pair<>(NullWritable.get(), new Text("JFK-[(MIA; 60)]-30-/MPX/HAV/MIA/JFK")),
        new Pair<>(NullWritable.get(), new Text("MIA-[(JFK; 10)]-20-/MPX/HAV/MIA")), 
        new Pair<>(NullWritable.get(), new Text("MPX-[(JFK; 190),(HAV; 10)]-0")));
  }

  /*
   * MPX-[(JFK; 190),(HAV; 80),(FCO; 40)]-0 JFK-[(MIA; 60)]-INF HAV-[(MIA;
   * 120),(LHR; 160),(JFK; 140)]-INF MIA-[(LHR; 135),(MPX; 110)]-INF LHR-[(DCA;
   * 165),(MPX; 50)]-INF DCA-[(FCO; 124),(CDG; 140)]-INF FCO-[(LHR; 85),(HAV;
   * 115),(CDG; 90)]-INF
   * 
   */

}
