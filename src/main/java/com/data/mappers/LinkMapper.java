package com.data.mappers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.bson.Document;

public class LinkMapper extends MapReduceBase implements Mapper<Object, Text, Text, List<Text>> {

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
	public void map(Object key, Text value, OutputCollector<Text, List<Text>> output, Reporter reporter)
			throws IOException {

		try {
			byte[] data = Files.readAllBytes(Paths.get(value.toString()));
			String jsonContent = new String(data);
			Document root = Document.parse(jsonContent);
			Document envelope = (Document) root.get("Envelope");
			if (envelope != null) {
				Document headerMetaData = (Document) envelope.get("WARC-Header-Metadata");
				if (headerMetaData != null) {
					String sourceURL = headerMetaData.getString("WARC-Target-URI");
					if (StringUtils.isNotBlank(sourceURL)) {
						Text hadoopSourceURL = new Text(sourceURL);
						List<Text> hadoopTargetURLs = new ArrayList<>();
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
												hadoopTargetURLs.add(new Text(url));
											}
										});
									}
								}
							}
						}

						// Sending to reducer
						output.collect(hadoopSourceURL, hadoopTargetURLs);
					}
				}
			}
		} catch (IOException ioException) {
			ioException.printStackTrace();
		}
	}
}
