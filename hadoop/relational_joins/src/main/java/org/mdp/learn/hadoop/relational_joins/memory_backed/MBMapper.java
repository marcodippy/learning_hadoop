package org.mdp.learn.hadoop.relational_joins.memory_backed;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MBMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
  private String              SEPARATOR;
  private Text                joinedRow = new Text();
  private Map<String, String> smallTable;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    SEPARATOR = context.getConfiguration().get("separator");
    smallTable = initSmallTableMap();
  }

  private Map<String, String> initSmallTableMap() throws IOException {
    return FileUtils.readLines(new File("./cachedSmallTable")).stream().collect(Collectors.toMap(line -> line.split(SEPARATOR)[0], line -> line));
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String joinKey = value.toString().split(SEPARATOR)[0];

    String smallTableRow = smallTable.get(joinKey);

    if (smallTableRow != null) {
      joinedRow.set(smallTableRow + SEPARATOR + value.toString());
      context.write(NullWritable.get(), joinedRow);
    }
  }

}
