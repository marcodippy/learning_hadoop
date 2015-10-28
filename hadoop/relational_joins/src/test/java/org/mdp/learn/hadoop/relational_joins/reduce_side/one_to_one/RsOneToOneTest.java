package org.mdp.learn.hadoop.relational_joins.reduce_side.one_to_one;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class RsOneToOneTest {
  private MapDriver<LongWritable, Text, JoinKey, Text>                           mapDriver;
  private ReduceDriver<JoinKey, Text, Text, NullWritable>                        reducerDriver;
  private MapReduceDriver<LongWritable, Text, JoinKey, Text, Text, NullWritable> mapRedDriver;

  private final String                                                           LEFT_TABLE  = "leftFile";
  private final String                                                           RIGHT_TABLE = "rightFile";

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    RsOneToOneMapper mapper = new RsOneToOneMapper();
    mapDriver = MapDriver.newMapDriver(mapper);

    mapDriver.getConfiguration().set(LEFT_TABLE + ".joinKeyIndex", "3");
    mapDriver.getConfiguration().set(RIGHT_TABLE + ".joinKeyIndex", "0");

    mapDriver.getConfiguration().set(LEFT_TABLE + ".joinOrder", "0");
    mapDriver.getConfiguration().set(RIGHT_TABLE + ".joinOrder", "1");

    RsOneToOneReducer reducer = new RsOneToOneReducer();
    reducerDriver = ReduceDriver.newReduceDriver(reducer);

    mapRedDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

    mapRedDriver.setKeyOrderComparator(new JoinKeyComparator());
    mapRedDriver.setKeyGroupingComparator(new JoinKeyGroupingComparator());
  }

  @Test
  public void testMapperWithLeftTable() throws IOException {
    Text leftRow = new Text("1;Marco;Italy;CDCMDP");

    mapDriver.setMapInputPath(new Path(LEFT_TABLE));
    mapDriver.withInput(new LongWritable(0), leftRow);
    mapDriver.withOutput(new JoinKey("CDCMDP", 0), leftRow);

    mapDriver.runTest();
  }

  @Test
  public void testMapperWithRightTable() throws IOException {
    Text rightRow = new Text("CDCMDP;Visa;4111111111111111;");

    mapDriver.setMapInputPath(new Path(RIGHT_TABLE));
    mapDriver.withInput(new LongWritable(0), rightRow);
    mapDriver.withOutput(new JoinKey("CDCMDP", 1), rightRow);

    mapDriver.runTest();
  }

  @Test
  public void testReduce() throws IOException {
    Text leftRow = new Text("1;Marco;Italy;CDCMDP");
    Text rightRow = new Text("CDCMDP;Visa;4111111111111111;");

    reducerDriver.withInput(new JoinKey("CDCMDP", 999), Arrays.asList(leftRow, rightRow));
    reducerDriver.withOutput(new Text(leftRow.toString() + ";" + rightRow.toString()), NullWritable.get());
    reducerDriver.runTest();
  }

}
