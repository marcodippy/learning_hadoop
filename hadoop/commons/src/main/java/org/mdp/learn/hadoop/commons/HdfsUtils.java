package org.mdp.learn.hadoop.commons;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public abstract class HdfsUtils {
	public static void deleteIfExists(Configuration config, Path outputFilePath) throws IOException {
		FileSystem fs = FileSystem.get(config);

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
	}

}
