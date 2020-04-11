package com.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
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
	 */
	public void extract(String zipFilePath) throws IOException {

		try (FSDataInputStream inputStream = fileSystem.open(new Path(zipFilePath));
				ZipInputStream zipInputStream = new ZipInputStream(inputStream)) {
			byte[] buffer = new byte[1024];
			ZipEntry zipEntry = zipInputStream.getNextEntry();
			while (zipEntry != null) {
				if (zipEntry.isDirectory()) {
					Path directoryPath = new Path(File.separator + zipEntry.getName());
					if (!fileSystem.exists(directoryPath)) {
						fileSystem.mkdirs(directoryPath);
					}
				} else {
					try (FSDataOutputStream outputStream = fileSystem
							.create(new Path(File.separator + zipEntry.getName()))) {
						int len;
						while ((len = zipInputStream.read(buffer)) > 0) {
							outputStream.write(buffer, 0, len);
						}
					}
				}
				zipEntry = zipInputStream.getNextEntry();
			}
		}
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
		if (zipFilePath.endsWith(".zip")) {
			Path path = new Path(zipFilePath);
			if (!fileSystem.exists(path)) {
				try (FSDataOutputStream outputStream = fileSystem.create(path)) {
					outputStream.write(IOUtils.toByteArray(url.openConnection()));
				}
			}
			return zipFilePath;
		}
		return "";
	}

	public void downloadAndProcessZip(String zipURL) throws IOException {

		String zipFileName = this.download(new URL(zipURL));
		this.extract(zipFileName);

		// Preparing files.txt
//		try (FileOutputStream fileOutputStream = new FileOutputStream(File.separator + "files.txt");
//				Stream<java.nio.file.Path> paths = Files
//						.list(Paths.get(File.separator + zipFileName.substring(0, zipFileName.lastIndexOf(".zip"))));) {
//			paths.filter(path -> path.toFile().isFile()).forEach(path -> {
//				try {
//					fileOutputStream.write(path.toFile().getAbsolutePath().getBytes());
//					fileOutputStream.write("\n".getBytes());
//				} catch (IOException ioException) {
//					System.err.println("Error adding the file path in files.txt");
//				}
//			});
//		}
	}

	public void deleteOldFiles() throws IOException {

		fileSystem.delete(new Path("/files.txt"), true);
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

		fileSystem = FileSystem.get(conf);
		this.deleteOldFiles();
		this.downloadAndProcessZip(args[0]);

		try {
//			JobClient.runJob(conf);
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
