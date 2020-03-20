package com.data.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.data.models.WebInfo;
import com.data.utils.StaleLinkChecker;

public class LinkReducer extends MapReduceBase implements Reducer<String, WebInfo, String, String> {

	/**
	 * Prepares the adjacency matrix from the web info list
	 * 
	 * @param webInfoList The web info list
	 * @return The adjacency matrix
	 */
	private int[][] prepareAdjacencyMatrix(List<WebInfo> webInfoList) {

		int[][] adjacency;
		int index = 0;
		HashMap<String, Integer> linkIndexMap = new HashMap<>();

		for (WebInfo webInfo : webInfoList) {
			String sourceURL = webInfo.getHadoopSourceURL();
			List<String> targetURLs = webInfo.getHadoopTargetURLs();

			if (!StaleLinkChecker.check(sourceURL)) {
				linkIndexMap.put(sourceURL, index++);
			}
			for (String targetURL : targetURLs) {
				if (!StaleLinkChecker.check(targetURL)) {
					linkIndexMap.put(targetURL, index++);
				}
			}
		}

		// Preparing the adjacency
		int totalLinks = linkIndexMap.size();
		adjacency = new int[totalLinks][totalLinks];
		for (WebInfo webInfo : webInfoList) {
			String sourceURL = webInfo.getHadoopSourceURL();
			List<String> targetURLs = webInfo.getHadoopTargetURLs();

			if (linkIndexMap.containsKey(sourceURL)) {
				for (String targetURL : targetURLs) {
					if (linkIndexMap.containsKey(targetURL)) {
						adjacency[linkIndexMap.get(sourceURL)][linkIndexMap.get(targetURL)] = 1;
					}
				}
			}
		}

		return adjacency;
	}

	/**
	 * Reduces the web info list
	 */
	@Override
	public void reduce(String key, Iterator<WebInfo> values, OutputCollector<String, String> output, Reporter reporter)
			throws IOException {

		List<WebInfo> webInfoList = new ArrayList<>();
		while (values.hasNext()) {
			webInfoList.add(values.next());
		}

		int[][] adjacencyMatrix = prepareAdjacencyMatrix(webInfoList);
	}
}
