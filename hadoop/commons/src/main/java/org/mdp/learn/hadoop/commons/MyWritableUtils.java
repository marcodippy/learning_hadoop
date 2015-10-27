package org.mdp.learn.hadoop.commons;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

public class MyWritableUtils {
  public static String mapToString(MapWritable map) {
    return map.entrySet().stream().map(entry -> "(" + entry.getKey() + " : " + entry.getValue() + ")").reduce((s1, s2) -> s1 + ", " + s2).get();
  }

  public static Text mapToText(MapWritable map) {
    return new Text(mapToString(map));
  }
}
