package org.mdp.learn.hadoop.average_grade_revisited;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class AverageGradeTest {
  private MapDriver<LongWritable, Text, CourseAndStudentWritable, Sum>                                                mapDriver;
  private ReduceDriver<CourseAndStudentWritable, Sum, CourseAndStudentWritable, FloatWritable>                        reducerDriver;
  private MapReduceDriver<LongWritable, Text, CourseAndStudentWritable, Sum, CourseAndStudentWritable, FloatWritable> mapRedDriver;
  private Counters                                                                                                    counters;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    AverageGradeMapper mapper = new AverageGradeMapper();
    mapDriver = MapDriver.newMapDriver(mapper);

    AverageGradeReducer reducer = new AverageGradeReducer();
    reducerDriver = ReduceDriver.newReduceDriver(reducer);

    counters = new Counters();

    mapRedDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    mapRedDriver.setKeyOrderComparator(new CourseAndStudentKeyComparator());
    mapRedDriver.setKeyGroupingComparator(new CourseAndStudentKeyComparator());
    mapRedDriver.withCounters(counters);
  }

  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new LongWritable(1), new Text("Algorithms;Fabio;66"));
    mapDriver.withInput(new LongWritable(2), new Text("Database;Marco;86"));
    mapDriver.withInput(new LongWritable(3), new Text("Algorithms;Fabio;63"));

    Sum algorithmsFabio = new Sum(66);
    algorithmsFabio.add(63);

    mapDriver.withOutput(new CourseAndStudentWritable("Algorithms", "Fabio"), algorithmsFabio);
    mapDriver.withOutput(new CourseAndStudentWritable("Database", "Marco"), new Sum(86));

    mapDriver.runTest();
    
    Counter malformedRows = mapDriver.getCounters().findCounter(MyCounters.MALFORMED_ROWS);
    Counter invalidGrades = mapDriver.getCounters().findCounter(MyCounters.INVALID_GRADES);
    
    assertThat(malformedRows.getValue()).isZero();
    assertThat(invalidGrades.getValue()).isZero();
  }

  @Test
  public void testMapper_WithMalformedInput() throws IOException {
    mapDriver.withInput(new LongWritable(1), new Text(";Fabio;66"));
    mapDriver.withInput(new LongWritable(2), new Text("Database;Marco;86"));
    mapDriver.withInput(new LongWritable(3), new Text("Fabio;63"));

    mapDriver.withOutput(new CourseAndStudentWritable("Database", "Marco"), new Sum(86));

    mapDriver.runTest();
    
    Counter malformedRows = mapDriver.getCounters().findCounter(MyCounters.MALFORMED_ROWS);
    Counter invalidGrades = mapDriver.getCounters().findCounter(MyCounters.INVALID_GRADES);
    
    assertThat(malformedRows.getValue()).isEqualTo(2);
    assertThat(invalidGrades.getValue()).isZero();
  }

  @Test
  public void testMapper_WithInvalidGradeInput() throws IOException {
    mapDriver.withInput(new LongWritable(1), new Text("Algorithms;Fabio;66"));
    mapDriver.withInput(new LongWritable(2), new Text("Database;Marco;aaaaaaaaa"));
    mapDriver.withInput(new LongWritable(3), new Text("Algorithms;Fabio;63"));
    mapDriver.withInput(new LongWritable(3), new Text("Algorithms;Fabio;120"));

    Sum algorithmsFabio = new Sum(66);
    algorithmsFabio.add(63);

    mapDriver.withOutput(new CourseAndStudentWritable("Algorithms", "Fabio"), algorithmsFabio);

    mapDriver.runTest();
    
    Counter malformedRows = mapDriver.getCounters().findCounter(MyCounters.MALFORMED_ROWS);
    Counter invalidGrades = mapDriver.getCounters().findCounter(MyCounters.INVALID_GRADES);
    
    assertThat(malformedRows.getValue()).isZero();
    assertThat(invalidGrades.getValue()).isEqualTo(2);
  }

  @Test
  public void testReducer() throws IOException {
    List<Sum> values = new ArrayList<Sum>();
    values.add(new Sum(50));
    values.add(new Sum(55));
    values.add(new Sum(51));

    reducerDriver.withInput(new CourseAndStudentWritable("Database", "Marco"), values);
    reducerDriver.withOutput(new CourseAndStudentWritable("Database", "Marco"), new FloatWritable(52f));

    reducerDriver.runTest();
  }

  @Test
  public void testMapReduce() throws IOException {
    mapRedDriver.withInput(new LongWritable(1), new Text("Algorithms;Fabio;66"));
    mapRedDriver.withInput(new LongWritable(2), new Text("Database;Marco;86"));
    mapRedDriver.withInput(new LongWritable(3), new Text("Algorithms;Fabio;63"));
    mapRedDriver.withInput(new LongWritable(4), new Text("Database;Marco;83"));
    mapRedDriver.withInput(new LongWritable(5), new Text("Algorithms;Fabio;70"));
    mapRedDriver.withInput(new LongWritable(6), new Text("Database;Fabio;53"));
    mapRedDriver.withInput(new LongWritable(7), new Text("Algorithms;Marco;76"));
    mapRedDriver.withInput(new LongWritable(8), new Text("Database;Marco;90"));
    mapRedDriver.withInput(new LongWritable(9), new Text("Algorithms;Marco;73"));
    mapRedDriver.withInput(new LongWritable(10), new Text("Database;Fabio;61"));
    mapRedDriver.withInput(new LongWritable(11), new Text("Algorithms;Marco;80"));
    mapRedDriver.withInput(new LongWritable(12), new Text("Database;Fabio;60"));

    mapRedDriver.addOutput(new CourseAndStudentWritable("Algorithms", "Marco"), new FloatWritable(76.333336f));
    mapRedDriver.addOutput(new CourseAndStudentWritable("Algorithms", "Fabio"), new FloatWritable(66.333336f));
    mapRedDriver.addOutput(new CourseAndStudentWritable("Database", "Marco"), new FloatWritable(86.333336f));
    mapRedDriver.addOutput(new CourseAndStudentWritable("Database", "Fabio"), new FloatWritable(60.5f));

    mapRedDriver.runTest(false);
  }
}
