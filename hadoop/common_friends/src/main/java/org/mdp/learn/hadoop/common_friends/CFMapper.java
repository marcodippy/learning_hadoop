package org.mdp.learn.hadoop.common_friends;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CFMapper extends Mapper<LongWritable, Text, Text, Text> {

  private Text mapKey     = new Text();
  private Text txtFriends = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split("-");
    String person = fields[0];
    String friendsString = fields[1];
    String[] friends = friendsString.split(",");

    for (String friend : friends) {
      if (person.compareTo(friend) < 0) 
        mapKey.set(person + ":" + friend);
      else
        mapKey.set(friend + ":" + person);

      txtFriends.set(friendsString);
      context.write(mapKey, txtFriends);
    }
  }

}
