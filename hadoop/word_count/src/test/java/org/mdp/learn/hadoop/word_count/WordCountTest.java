package org.mdp.learn.hadoop.word_count;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import static org.assertj.core.api.Assertions.*;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Objects;

public class WordCountTest {

	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reducerDriver;
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapRedDriver;

	@Before
	public void setUp() throws Exception {
		WordCountMapper mapper = new WordCountMapper();
		mapDriver = MapDriver.newMapDriver(mapper);

		WordCountReducer reducer = new WordCountReducer();
		reducerDriver = ReduceDriver.newReduceDriver(reducer);

		mapRedDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text("to be or not to be"));

		mapDriver.withOutput(new Text("to"), new IntWritable(1));
		mapDriver.withOutput(new Text("be"), new IntWritable(1));
		mapDriver.withOutput(new Text("or"), new IntWritable(1));
		mapDriver.withOutput(new Text("not"), new IntWritable(1));
		mapDriver.withOutput(new Text("to"), new IntWritable(1));
		mapDriver.withOutput(new Text("be"), new IntWritable(1));

		mapDriver.runTest();
	}

	@Test
	public void testMapper_WithAssertions() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text("to be or not to be"));

		List<Pair<Text, IntWritable>> result = mapDriver.run();
		
		List<Pair<Text, IntWritable>> expectedOutput = Arrays.asList(
					new Pair<Text, IntWritable>(new Text("to"), new IntWritable(1)),
					new Pair<Text, IntWritable>(new Text("be"), new IntWritable(1)),
					new Pair<Text, IntWritable>(new Text("or"), new IntWritable(1)),
					new Pair<Text, IntWritable>(new Text("not"), new IntWritable(1)),
					new Pair<Text, IntWritable>(new Text("to"), new IntWritable(1)),
					new Pair<Text, IntWritable>(new Text("be"), new IntWritable(1))
				);
	
		assertThat(result)
			.isNotNull()
			.hasSize(6)
			.containsExactlyElementsOf(expectedOutput);
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));

		reducerDriver.withInput(new Text("to"), values);
		reducerDriver.withOutput(new Text("to"), new IntWritable(2));

		reducerDriver.runTest();
	}

	@Test
	public void testMapReduce() throws IOException {
		mapRedDriver.withInput(new LongWritable(1), new Text("to be or not to be"));

		mapRedDriver.addOutput(new Text("be"), new IntWritable(2));
		mapRedDriver.addOutput(new Text("not"), new IntWritable(1));
		mapRedDriver.addOutput(new Text("or"), new IntWritable(1));
		mapRedDriver.addOutput(new Text("to"), new IntWritable(2));

		mapRedDriver.runTest();
	}
}
