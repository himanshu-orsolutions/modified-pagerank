package com.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.data.utils.ZipExtractor;
import com.data.utils.ZipFileDownloader;

/**
 * The TaskExecutor. It holds implementation to execute the map-reduce task.
 */
public class TaskExecutor extends Configured implements Tool {

	/**
	 * Preapares a file which holds list of all files present in the zip directory
	 * 
	 * @param directoryPath The directory path
	 * @throws IOException
	 */
	private void prepareFilesList(Path directoryPath) throws IOException {

		if (directoryPath.toFile().isDirectory()) {
			File[] files = directoryPath.toFile().listFiles((file) -> !file.isDirectory());
			try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("files.txt"))) {
				for (File file : files) {
					bufferedWriter.write(file.getAbsolutePath());
					bufferedWriter.write("\n");
				}
			}
		}
	}

	public int run(String[] args) throws Exception {

		if (args.length != 1) { // Checking for command line arguments
			System.err.printf("Invalid arguments count");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		String url = args[0];
		String zipFileName = ZipFileDownloader.download(new URL(url));
		if (!"".equals(zipFileName)) {
			ZipExtractor.extract(Paths.get(zipFileName));
			this.prepareFilesList(Paths.get(zipFileName.substring(0, zipFileName.lastIndexOf(".zip"))));
		}
		return 0;
	}

	/**
	 * The execution starts from here
	 * 
	 * @param args The command line arguments
	 * @throws Exception
	 */
	public static void main(String[] args) {

		try {
			int exitCode = ToolRunner.run(new TaskExecutor(), args);
			System.exit(exitCode);
		} catch (Exception exception) {
			exception.printStackTrace();
			System.out.println("Error executing the map reduce task.");
		}
	}
}
