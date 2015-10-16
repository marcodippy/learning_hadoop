package org.mdp.learn.hadoop.average_grade;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class AverageGradeTest {
//	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
//	private ReduceDriver<Text, IntPair, Text, IntWritable> reducerDriver;
//	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapRedDriver;

//	@Before
//	public void setUp() throws Exception {
//		AverageGradeMapper mapper = new AverageGradeMapper();
//		mapDriver = MapDriver.newMapDriver(mapper);
//
//		AverageGradeReducer reducer = new AverageGradeReducer();
//		reducerDriver = ReduceDriver.newReduceDriver(reducer);
//
//		mapRedDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
//	}
//
//	@Test
//	public void testMapper() throws IOException {
//		mapDriver.withInput(new LongWritable(1), new Text("to be or not to be"));
//
//		mapDriver.withOutput(new Text("to"), new IntWritable(1));
//		mapDriver.withOutput(new Text("be"), new IntWritable(1));
//		mapDriver.withOutput(new Text("or"), new IntWritable(1));
//		mapDriver.withOutput(new Text("not"), new IntWritable(1));
//		mapDriver.withOutput(new Text("to"), new IntWritable(1));
//		mapDriver.withOutput(new Text("be"), new IntWritable(1));
//
//		mapDriver.runTest();
//	}
}
