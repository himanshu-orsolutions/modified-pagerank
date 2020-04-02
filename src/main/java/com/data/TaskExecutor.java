package com.data;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.data.mappers.LinkMapper;
import com.data.reducers.LinkReducer;
import com.data.utils.ZipExtractor;
import com.data.utils.ZipFileDownloader;

/**
 * The TaskExecutor. It holds implementation to execute the map-reduce task.
 */
public class TaskExecutor extends Configured implements Tool {

	public void downloadAndProcessZip(String zipURL) throws IOException {

		String zipFileName = ZipFileDownloader.download(new URL(zipURL));
		ZipExtractor.extract(Paths.get(zipFileName));

		// Preparing files.txt
		try (FileOutputStream fileOutputStream = new FileOutputStream("files.txt");
				Stream<java.nio.file.Path> paths = Files
						.list(Paths.get(zipFileName.substring(0, zipFileName.lastIndexOf(".zip"))));) {
			paths.filter(path -> path.toFile().isFile()).forEach(path -> {
				try {
					fileOutputStream.write(path.toFile().getAbsolutePath().getBytes());
					fileOutputStream.write("\n".getBytes());
				} catch (IOException ioException) {
					System.err.println("Error adding the file path in files.txt");
				}
			});
		}
	}

	public void uploadMetadataToHadoop() throws IOException {

		Runtime.getRuntime().exec("hdfs dfs -rm /files.txt");
		Runtime.getRuntime().exec("hdfs dfs -put files.txt /");
	}

	public int run(String[] args) throws Exception {

		if (args.length != 1) { // Checking for command line arguments
			System.err.printf("Invalid arguments count");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		this.downloadAndProcessZip(args[0]);
		this.uploadMetadataToHadoop();

		JobConf conf = new JobConf(TaskExecutor.class);
		conf.setJobName("Modified Page Rank");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setMapperClass(LinkMapper.class);
		conf.setReducerClass(LinkReducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("/files.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("/modified-pagerank"));

		try {
			JobClient.runJob(conf);
		} catch (Exception exception) {
			exception.printStackTrace();
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
