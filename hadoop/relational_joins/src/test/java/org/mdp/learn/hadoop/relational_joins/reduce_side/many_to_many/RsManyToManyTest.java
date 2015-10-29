package org.mdp.learn.hadoop.relational_joins.reduce_side.many_to_many;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.mdp.learn.hadoop.relational_joins.reduce_side.one_to_many.RsOneToManyReducer;
import org.mdp.learn.hadoop.relational_joins.reduce_side.one_to_one.JoinKey;

public class RsManyToManyTest {
  private ReduceDriver<JoinKey, Text, Text, NullWritable> reducerDriver;

  @Before
  public void setUp() throws Exception {
    RsOneToManyReducer reducer = new RsOneToManyReducer();
    reducerDriver = ReduceDriver.newReduceDriver(reducer);
  }

  @Test
  public void testReduce() throws IOException {
    reducerDriver.withInput(new JoinKey("1", 1), Arrays.asList(new Text("1;Marco;Italy;CDCMDP"), new Text("1;MarcoClone;Italy;CDCMDP")))
                    .withInput(new JoinKey("1", 2), Arrays.asList(new Text("45678;1;Milan-Catania;24/06/2016"), 
                                                                  new Text("23456;1;New York-Havana;02/06/2016")));
    
    reducerDriver.withInput(new JoinKey("3", 1), Arrays.asList(new Text("3;Gordon;Scotland;GRDXWV"), new Text("3;GordonClone;Scotland;GRDXWV")))
                    .withInput(new JoinKey("3", 2), Arrays.asList(new Text("12345;3;Milan-New York;25/06/2016"), 
                                                                  new Text("34567;3;London-San Francisco;22/06/2016"),
                                                                  new Text("34567;3;Rome-Moscow;27/06/2016")));
    
    reducerDriver.withOutput(new Text("1;Marco;Italy;CDCMDP;45678;1;Milan-Catania;24/06/2016"), NullWritable.get());
    reducerDriver.withOutput(new Text("1;Marco;Italy;CDCMDP;23456;1;New York-Havana;02/06/2016"), NullWritable.get());
    reducerDriver.withOutput(new Text("1;MarcoClone;Italy;CDCMDP;45678;1;Milan-Catania;24/06/2016"), NullWritable.get());
    reducerDriver.withOutput(new Text("1;MarcoClone;Italy;CDCMDP;23456;1;New York-Havana;02/06/2016"), NullWritable.get());
    reducerDriver.withOutput(new Text("3;Gordon;Scotland;GRDXWV;12345;3;Milan-New York;25/06/2016"), NullWritable.get());
    reducerDriver.withOutput(new Text("3;Gordon;Scotland;GRDXWV;34567;3;London-San Francisco;22/06/2016"), NullWritable.get());
    reducerDriver.withOutput(new Text("3;Gordon;Scotland;GRDXWV;34567;3;Rome-Moscow;27/06/2016"), NullWritable.get());
    reducerDriver.withOutput(new Text("3;GordonClone;Scotland;GRDXWV;12345;3;Milan-New York;25/06/2016"), NullWritable.get());
    reducerDriver.withOutput(new Text("3;GordonClone;Scotland;GRDXWV;34567;3;London-San Francisco;22/06/2016"), NullWritable.get());
    reducerDriver.withOutput(new Text("3;GordonClone;Scotland;GRDXWV;34567;3;Rome-Moscow;27/06/2016"), NullWritable.get());
    
    reducerDriver.runTest();
  }

}
