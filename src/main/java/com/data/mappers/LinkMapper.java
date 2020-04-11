package com.data.mappers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.bson.Document;

import com.data.models.WebInfo;
import com.google.gson.Gson;

public class LinkMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {

	/**
	 * Checks if the URL should be included in the link map
	 * 
	 * @param url The URL
	 * @return The status
	 */
	private boolean isIncluded(String url) {

		String file = url.substring(url.lastIndexOf("/") + 1);
		String format = file.substring(file.lastIndexOf(".") + 1);
		return (url.startsWith("http") || url.startsWith("https"))
				&& (StringUtils.isBlank(format) || StringUtils.equalsAny("stm", "htm", "html", "shtml", format));
	}

	/**
	 * Maps the links
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		FileSystem fileSystem = FileSystem.get(new JobConf());
		try (FSDataInputStream inputStream = fileSystem.open(new Path(value.toString()));
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			System.out.println(String.format("WAT file path: %s", value.toString()));

			int len;
			byte[] buffer = new byte[1024];
			while ((len = inputStream.read(buffer)) > 0) {
				outputStream.write(buffer, 0, len);
			}

			byte[] data = outputStream.toByteArray();
			String jsonContent = new String(data);
			Document root = Document.parse(jsonContent);
			Document envelope = (Document) root.get("Envelope");
			if (envelope != null) {
				Document headerMetaData = (Document) envelope.get("WARC-Header-Metadata");
				if (headerMetaData != null) {
					String sourceURL = headerMetaData.getString("WARC-Target-URI");
					if (StringUtils.isNotBlank(sourceURL)) {
						List<String> targetURLs = new ArrayList<>();
						Document payloadMetaData = (Document) envelope.get("Payload-Metadata");
						if (payloadMetaData != null) {
							Document httpResponseMetadata = (Document) payloadMetaData.get("HTTP-Response-Metadata");
							if (httpResponseMetadata != null) {
								Document htmlMetaData = (Document) httpResponseMetadata.get("HTML-Metadata");
								if (htmlMetaData != null) {
									List<Document> links = (List<Document>) htmlMetaData.get("Links");
									if (links != null && !links.isEmpty()) {
										links.forEach(link -> {
											String url = link.getString("url");
											if (StringUtils.isNotBlank(url) && isIncluded(url)) {
												targetURLs.add(url);
											}
										});
									}
								}
							}
						}

						// Sending to reducer
						if (!targetURLs.isEmpty()) {
							System.out.println(String.format("Found %d outgoing URLs", targetURLs.size()));
							output.collect(new Text("web-graph"),
									new Text(new Gson().toJson(new WebInfo(sourceURL, targetURLs))));
						}
					}
				}
			}
		} catch (IOException ioException) {
			ioException.printStackTrace();
		}
	}
}
