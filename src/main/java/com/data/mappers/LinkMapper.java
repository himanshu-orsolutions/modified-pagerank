package com.data.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.bson.Document;

import com.data.models.WebInfo;
import com.google.gson.Gson;

public class LinkMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {

	private int counter = 0;

	/**
	 * Checks if the URL should be included in the link map
	 * 
	 * @param url The URL
	 * @return The status
	 */
	private boolean isIncluded(String url) {

		if (url != null) {
			String file = url.substring(url.lastIndexOf("/") + 1);
			String format = file.substring(file.lastIndexOf(".") + 1);
			return (url.startsWith("http") || url.startsWith("https")) && ("".equals(format) || "stm".equals(format)
					|| "htm".equals(format) || "html".equals(format) || "shtml".equals(format));
		}
		return false;
	}

	/**
	 * Maps the links
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		if (value.toString().matches("\\{\\\"Container\\\"\\:.*\\}")) {
			Document root = Document.parse(value.toString());
			Document envelope = (Document) root.get("Envelope");
			if (envelope != null) {
				Document headerMetaData = (Document) envelope.get("WARC-Header-Metadata");
				if (headerMetaData != null) {
					String sourceURL = headerMetaData.getString("WARC-Target-URI");
					if (!"".equals(sourceURL)) {
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
											if (!"".equals(url) && isIncluded(url)) {
												targetURLs.add(url);
											}
										});
									}
								}
							}
						}

						// Sending to reducer
						if (counter++ < 5000) {
							output.collect(new Text("web-graph"),
									new Text(new Gson().toJson(new WebInfo(sourceURL, targetURLs))));
						}
					}
				}
			}
		}
	}
}
