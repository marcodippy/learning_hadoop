package org.mdp.learn.hadoop.average_grade_revisited;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageGradeMapper extends Mapper<LongWritable, Text, CourseAndStudentWritable, Sum> {

  private final Map<CourseAndStudentWritable, Sum> map    = new HashMap<>();
  private final RecordParser                       parser = new RecordParser();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    parser.parse(value);

    if (parser.isValidRecord()) {
      CourseAndStudentWritable mapKey = new CourseAndStudentWritable(parser.getCourse(), parser.getStudent());

      if (map.containsKey(mapKey)) {
        map.get(mapKey).add(parser.getGrade());
      }
      else {
        map.put(mapKey, new Sum(parser.getGrade()));
      }
    }
    else {
      if (!parser.isWellFormed()) {
        context.getCounter(MyCounters.MALFORMED_ROWS).increment(1);
        context.setStatus("Detected possibly corrupt record");
      }
      else if (!parser.isValidGrade()) {
        context.setStatus("Detected invalid grade in a record");
        context.getCounter(MyCounters.INVALID_GRADES).increment(1);
      }
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    Iterator<Entry<CourseAndStudentWritable, Sum>> iterator = map.entrySet().iterator();

    while (iterator.hasNext()) {
      Entry<CourseAndStudentWritable, Sum> entry = iterator.next();
      context.write(entry.getKey(), entry.getValue());
    }
  }

}