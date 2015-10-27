package org.mdp.learn.hadoop.commons;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

public class TestUtils {
  public static List<Pair<LongWritable, Text>> getInput(Path path) throws IOException {
    return Files.readAllLines(path).stream()
        .map(line -> new Pair<LongWritable, Text>(new LongWritable(0), new Text(line)))
        .collect(Collectors.toList());
  }
}
