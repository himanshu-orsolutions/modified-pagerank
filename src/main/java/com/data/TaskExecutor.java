package com.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
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

/**
 * The TaskExecutor. It holds implementation to execute the map-reduce task.
 */
public class TaskExecutor extends Configured implements Tool {

	private FileSystem fileSystem;

	/**
	 * Extracts the zip file
	 * 
	 * @param zipFilePath The zip file path
	 * @throws IOException
	 * @throws FileNotFoundException
	 * @return The list of files paths present in the zip
	 */
	public Path extract(String zipFilePath) throws IOException {

		JobConf jobConf = new JobConf();
		Path inputPath = new Path(zipFilePath);
		CompressionCodecFactory factory = new CompressionCodecFactory(jobConf);
		CompressionCodec codec = factory.getCodec(inputPath);
		Path outputPath = new Path(CompressionCodecFactory.removeSuffix(zipFilePath, codec.getDefaultExtension()));

		try (InputStream inputStream = codec.createInputStream(fileSystem.open(inputPath));
				OutputStream outputStream = fileSystem.create(outputPath);) {

			IOUtils.copyBytes(inputStream, outputStream, jobConf);
		}

		return outputPath;
	}

	/**
	 * Gets the file name
	 * 
	 * @param url The zip file URL
	 * @return The file name
	 */
	private String getFileName(URL url) {
		return url.getPath().substring(url.getPath().lastIndexOf("/") + 1);
	}

	/**
	 * Downloads the zip file
	 * 
	 * @param url The URL
	 * @return The zip file path
	 * @throws IOException
	 */
	public String download(URL url) throws IOException {

		String zipFilePath = File.separator + getFileName(url);
		if (zipFilePath.endsWith(".gz")) {
			Path path = new Path(zipFilePath);
			if (!fileSystem.exists(path)) {
				try (FSDataOutputStream outputStream = fileSystem.create(path);
						InputStream inputStream = url.openStream()) {
					int len;
					byte[] buffer = new byte[1024];
					while ((len = inputStream.read(buffer)) > 0) {
						outputStream.write(buffer, 0, len);
					}
				}
			}
			return zipFilePath;
		}
		return "";
	}

	public Path downloadAndProcessZip(String zipURL) throws IOException {

		String zipFileName = this.download(new URL(zipURL));
		return this.extract(zipFileName);
	}

	public void deleteOldFiles() throws IOException {

		fileSystem.delete(new Path("/modified-pagerank"), true);
	}

	public int run(String[] args) throws Exception {

		if (args.length != 1) { // Checking for command line arguments
			System.err.printf("Invalid arguments count");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		JobConf conf = new JobConf(TaskExecutor.class);
		conf.setJobName("Modified Page Rank");
		conf.setNumReduceTasks(5);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setMapperClass(LinkMapper.class);
		conf.setReducerClass(LinkReducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(conf, new Path("/modified-pagerank"));

		fileSystem = FileSystem.get(conf);
		this.deleteOldFiles();
		Path inputPath = this.downloadAndProcessZip(args[0]);
		FileInputFormat.setInputPaths(conf, inputPath);

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
