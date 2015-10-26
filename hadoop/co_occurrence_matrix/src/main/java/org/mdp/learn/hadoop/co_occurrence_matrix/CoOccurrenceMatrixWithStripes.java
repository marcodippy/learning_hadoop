package org.mdp.learn.hadoop.co_occurrence_matrix;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.learn.hadoop.commons.HdfsUtils;
import org.mdp.learn.hadoop.commons.JobBuilder;

public class CoOccurrenceMatrixWithStripes extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);

    job.setMapperClass(CoOccurrenceMatrixWithStripesMapper.class);
    job.setReducerClass(CoOccurrenceMatrixWithStripesReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritable.class);

    HdfsUtils.deleteIfExists(getConf(), new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new CoOccurrenceMatrixWithStripes(), args));
  }

  public class CoOccurrenceMatrixWithStripesMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private final Text        word       = new Text();
    private final IntWritable ZERO       = new IntWritable(0);
    private int               neighbours = 2;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] words = value.toString().replaceAll("--", "").toLowerCase().split("[\\s,;.:?!]+");

      for (int i = 0; i < words.length; i++) {
        MapWritable map = new MapWritable();

        for (int k = i - neighbours; k < i + neighbours; i++) {
          if (k == i || k < 0 || k > words.length - 1) continue;
          incrementCount(map, new Text(words[k]));
        }

        word.set(words[i]);
        context.write(word, map);
      }
    }

    private void incrementCount(MapWritable map, Text key) {
      IntWritable count = (IntWritable) map.getOrDefault(key, ZERO);
      count.set(count.get() + 1);
      map.put(key, count);
    }
  }

  public class CoOccurrenceMatrixWithStripesReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
    private final IntWritable ZERO = new IntWritable(0);

    @Override
    protected void reduce(Text key, Iterable<MapWritable> stripes, Context context) throws IOException, InterruptedException {
      MapWritable map = new MapWritable();

      for (MapWritable stripe : stripes)
        stripe.forEach((neighbour, count) -> updateCount(map, (Text) neighbour, (IntWritable) count));

      context.write(key, map);
    }

    private void updateCount(MapWritable map, Text key, IntWritable cnt) {
      IntWritable count = (IntWritable) map.getOrDefault(key, ZERO);
      count.set(count.get() + cnt.get());
      map.put(key, count);
    }
  }
}
