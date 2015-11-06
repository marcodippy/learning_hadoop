package org.mdp.learn.hadoop.common_friends;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CFReducer extends Reducer<Text, Text, NullWritable, Text> {

  private Text val = new Text();

  @Override
  protected void reduce(Text pair, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    Iterator<Text> it = values.iterator();
    List<String> friendsOfPerson1 = Arrays.asList(it.next().toString().split(","));
    List<String> friendsOfPerson2 = Arrays.asList(it.next().toString().split(","));

    String commonFriends = friendsOfPerson1.stream().filter(friendsOfPerson2::contains).collect(Collectors.joining(","));
    val.set(pair.toString() + " - " + commonFriends);
    context.write(NullWritable.get(), val);
  }

}
