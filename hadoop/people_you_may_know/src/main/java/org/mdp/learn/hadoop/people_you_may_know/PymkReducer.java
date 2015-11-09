package org.mdp.learn.hadoop.people_you_may_know;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.mdp.learn.hadoop.commons.TextPair;

public class PymkReducer extends Reducer<Text, TextPair, Text, Text> {
  private Text output = new Text();

  @Override
  protected void reduce(Text key, Iterable<TextPair> values, Context context) throws IOException, InterruptedException {
    Map<String, List<String>> mutualFriends = new HashMap<>();

    for (TextPair pair : values) {
      String toUser = pair.getLeft().toString();
      String mutualFriend = pair.getRight().toString();
      boolean alreadyFriend = "FRIENDS".equals(mutualFriend);

      if (mutualFriends.containsKey(toUser)) {
        if (alreadyFriend) {
          mutualFriends.put(toUser, null);
        }
        else if (mutualFriends.get(toUser) != null) {
          mutualFriends.get(toUser).add(mutualFriend);
        }
      }
      else {
        mutualFriends.put(toUser, alreadyFriend ? null : new ArrayList<>(Arrays.asList(mutualFriend)));
      }
    }

    String out = buildOutput(mutualFriends);
    if (!out.isEmpty()) {
      output.set(out);
      context.write(key, output);
    }
  }

  private String buildOutput(Map<String, List<String>> map) {
    return map.entrySet().stream().filter(e -> e.getValue() != null).map((e) -> e.getKey() + "(" + e.getValue().size() + ":" + e.getValue() + ")")
        .collect(Collectors.joining(","));
  }

}
