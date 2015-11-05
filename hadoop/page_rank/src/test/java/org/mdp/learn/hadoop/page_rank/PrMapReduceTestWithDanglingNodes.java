package org.mdp.learn.hadoop.page_rank;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import static org.junit.Assert.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mdp.learn.hadoop.page_rank.PrConstants.PrCounters;

public class PrMapReduceTestWithDanglingNodes {
  private MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> mapRedDriver;
  private MapDriver<LongWritable, Text, NullWritable, Text>                   redistributeLostPageRank;

  private static final Long MAX_ITERATIONS = 20l;
  
  @Before
  public void setUp() throws Exception {
    mapRedDriver = MapReduceDriver.newMapReduceDriver(new PrMapper(), new PrReducer());
    mapRedDriver.withCounters(new Counters());
    
    redistributeLostPageRank = MapDriver.newMapDriver(new PrRedistributeLostPageRankMapper());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testJobWithDanglingNodes() throws Exception {
    
    List<Pair<LongWritable, Text>> inputs = Arrays.asList(
        new Pair<>(new LongWritable(0), new Text("n1;[n2, n4];0.2")),
        new Pair<>(new LongWritable(0), new Text("n2;[n3, n5];0.2")),
        new Pair<>(new LongWritable(0), new Text("n3;[n4];0.2")),
        new Pair<>(new LongWritable(0), new Text("n4;[];0.2")),
        new Pair<>(new LongWritable(0), new Text("n5;[n1, n2, n3];0.2"))
        );

    List<Pair<NullWritable, Text>> ret = null;
    long iterations = 0, changedNodes = 1, numberOfNodes = inputs.size(), lostPageRankMass = -1;

    do {
      mapRedDriver.getConfiguration().setLong("number_of_nodes",  numberOfNodes);
      ret = mapRedDriver.withAll(inputs).run();
      
      changedNodes = mapRedDriver.getCounters().findCounter(PrCounters.CHANGED_PAGE_RANKS).getValue();
      lostPageRankMass = mapRedDriver.getCounters().findCounter(PrCounters.LOST_PAGE_RANK_MASS).getValue();
      
      ret.forEach(System.out::println);
      System.out.println("page rank changes " + changedNodes);
      System.out.println("lost page rank mass " + lostPageRankMass);
      
      inputs = ret.stream().map(x -> new Pair<LongWritable, Text>(new LongWritable(0), x.getSecond())).collect(Collectors.toList());   
      
      if (lostPageRankMass > 0) { // REDISTRIBUTE LOST PAGE RANK MASS
        System.out.println("\n************* RPR *************");
        
        redistributeLostPageRank.getConfiguration().setLong("lost_page_rank_mass", lostPageRankMass);
        redistributeLostPageRank.getConfiguration().setLong("number_of_nodes", numberOfNodes);
        redistributeLostPageRank.addAll(inputs);
        
        ret = redistributeLostPageRank.run();
        changedNodes = mapRedDriver.getCounters().findCounter(PrCounters.CHANGED_PAGE_RANKS).getValue();
        
        ret.forEach(s -> System.out.println("\t"+s));
        System.out.println("page rank changes " + changedNodes);
        
        inputs = ret.stream().map(x -> new Pair<LongWritable, Text>(new LongWritable(0), x.getSecond())).collect(Collectors.toList());   
        
        System.out.println("\n*********** END RPR ***************");
      } else {
        fail("there shoudl be loss of page rank with dangling nodes!");
      }
        
      iterations++;
      setUp();
    }
    while (changedNodes > 0 && iterations < MAX_ITERATIONS);

    System.out.println("MapReduce terminated in " + iterations + " iterations");

    assertThat(iterations).isEqualTo(20);

    assertThat(ret).contains(
          new Pair<>(NullWritable.get(), new Text("n1;[n2,n4];0.1604288498936947")),
          new Pair<>(NullWritable.get(), new Text("n2;[n3,n5];0.21838319695592132")),
          new Pair<>(NullWritable.get(), new Text("n3;[n4];0.23931875235705202")),
          new Pair<>(NullWritable.get(), new Text("n4;[];0.344839040867764")),
          new Pair<>(NullWritable.get(), new Text("n5;[n1,n2,n3];0.1928699024633573"))
        );
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testJobWithoutDanglingNodes() throws Exception {
    
    List<Pair<LongWritable, Text>> inputs = Arrays.asList(
        new Pair<>(new LongWritable(0), new Text("n1;[n2, n4];0.2")),
        new Pair<>(new LongWritable(0), new Text("n2;[n3, n5];0.2")),
        new Pair<>(new LongWritable(0), new Text("n3;[n4];0.2")),
        new Pair<>(new LongWritable(0), new Text("n4;[n5];0.2")),
        new Pair<>(new LongWritable(0), new Text("n5;[n1, n2, n3];0.2"))
        );

    List<Pair<NullWritable, Text>> ret = null;
    long iterations = 0, changedNodes = 1, numberOfNodes = inputs.size(), lostPageRankMass = -1;

    do {
      mapRedDriver.getConfiguration().setLong("number_of_nodes",  numberOfNodes);
      ret = mapRedDriver.withAll(inputs).run();
      
      changedNodes = mapRedDriver.getCounters().findCounter(PrCounters.CHANGED_PAGE_RANKS).getValue();
      lostPageRankMass = mapRedDriver.getCounters().findCounter(PrCounters.LOST_PAGE_RANK_MASS).getValue();
      
      ret.forEach(System.out::println);
      System.out.println("page rank changes " + changedNodes);
      System.out.println("lost page rank mass " + lostPageRankMass);
      
      inputs = ret.stream().map(x -> new Pair<LongWritable, Text>(new LongWritable(0), x.getSecond())).collect(Collectors.toList());   
      
      if (lostPageRankMass > 0) {
        fail("There shouldn't be loss of page rank without dangling nodes!");
      }
        
      iterations++;
      setUp();
    }
    while (changedNodes > 0 && iterations < MAX_ITERATIONS);

    System.out.println("MapReduce terminated in " + iterations + " iterations");
    
    assertThat(iterations).isEqualTo(15);

    assertThat(ret).contains(
          new Pair<>(NullWritable.get(), new Text("n1;[n2,n4];0.11507753611082788")),
          new Pair<>(NullWritable.get(), new Text("n2;[n3,n5];0.1639925786843643")),
          new Pair<>(NullWritable.get(), new Text("n3;[n4];0.1847646984689828")),
          new Pair<>(NullWritable.get(), new Text("n4;[n5];0.23596497880283826")),
          new Pair<>(NullWritable.get(), new Text("n5;[n1,n2,n3];0.30020020793298685"))
        );
  }

}
