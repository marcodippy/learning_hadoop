package org.mdp.learn.hadoop.common_friends;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class CommonFriendsTest {
  private MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> mapRedDriver;

  @Before
  public void setUp() throws Exception {
    mapRedDriver = MapReduceDriver.newMapReduceDriver(new CFMapper(), new CFReducer());
  }

  @Test
  public void testMapReduce() throws IOException {
    mapRedDriver.withInput(new LongWritable(1), new Text("Marco-Fabio,Luca,Gordon"));
    mapRedDriver.withInput(new LongWritable(2), new Text("Fabio-Nathalia,Marco,Luca,Gordon"));
    mapRedDriver.withInput(new LongWritable(3), new Text("Luca-Nathalia,Marco,Fabio"));
    mapRedDriver.withInput(new LongWritable(4), new Text("Nathalia-Fabio,Luca"));
    mapRedDriver.withInput(new LongWritable(5), new Text("Gordon-Marco,Fabio"));

    mapRedDriver.withOutput(new Pair<NullWritable, Text>(NullWritable.get(), new Text("Fabio:Gordon - Marco")));
    mapRedDriver.withOutput(new Pair<NullWritable, Text>(NullWritable.get(), new Text("Fabio:Luca - Nathalia,Marco")));
    mapRedDriver.withOutput(new Pair<NullWritable, Text>(NullWritable.get(), new Text("Fabio:Marco - Luca,Gordon")));
    mapRedDriver.withOutput(new Pair<NullWritable, Text>(NullWritable.get(), new Text("Fabio:Nathalia - Luca")));
    mapRedDriver.withOutput(new Pair<NullWritable, Text>(NullWritable.get(), new Text("Gordon:Marco - Fabio")));
    mapRedDriver.withOutput(new Pair<NullWritable, Text>(NullWritable.get(), new Text("Luca:Marco - Fabio")));
    mapRedDriver.withOutput(new Pair<NullWritable, Text>(NullWritable.get(), new Text("Luca:Nathalia - Fabio")));

    mapRedDriver.runTest();
  }
}
