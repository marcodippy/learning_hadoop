package org.mdp.learn.hadoop.people_you_may_know;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mdp.learn.hadoop.commons.TextPair;

public class PymkMapper extends Mapper<LongWritable, Text, Text, TextPair> {
  private TextPair pair = new TextPair();
  private Text     mkey = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split("-");
    String person = fields[0];
    List<String> friends = Arrays.asList(fields[1].split(","));

    for (String friend : friends) {
      mkey.set(person);
      context.write(mkey, pair.set(friend, "FRIENDS"));
    }

    for (int i = 0; i < friends.size(); i++) {
      for (int j = i + 1; j < friends.size(); j++) {
        mkey.set(friends.get(j));
        context.write(mkey, pair.set(friends.get(i), person));

        mkey.set(friends.get(i));
        context.write(mkey, pair.set(friends.get(j), person));
      }
    }
  }
}
