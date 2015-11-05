package org.mdp.learn.hadoop.commons;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public abstract class HdfsUtils {
  public static void deleteIfExists(Configuration config, Path outputFilePath) {
    try {
      FileSystem fs = FileSystem.get(config);

      if (fs.exists(outputFilePath)) {
        fs.delete(outputFilePath, true);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void deleteIfExists(Configuration config, List<Path> filesToDelete) {
    filesToDelete.forEach(path -> deleteIfExists(config, path));
  }

  public static void rename(Configuration config, Path toRename, Path newPath) {
    try {
      FileSystem fs = FileSystem.get(config);

      if (fs.exists(toRename)) {
        fs.rename(toRename, newPath);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
