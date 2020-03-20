package com.data.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.data.models.Connection;
import com.data.models.WebInfo;
import com.data.utils.LoopDetector;
import com.data.utils.StaleLinkChecker;

public class LinkReducer extends MapReduceBase implements Reducer<String, WebInfo, String, Double> {

	/**
	 * Gets the link index map
	 * 
	 * @param webInfoList The web info list
	 * @return The link index map
	 */
	private HashMap<String, Integer> getLinkIndexMap(List<WebInfo> webInfoList) {

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

		return linkIndexMap;
	}

	/**
	 * Prepares the adjacency matrix
	 * 
	 * @param webInfoList  The web info list
	 * @param linkIndexMap The link index map
	 * @return The adjacency matrix
	 */
	private int[][] prepareAdjacencyMatrix(List<WebInfo> webInfoList, HashMap<String, Integer> linkIndexMap) {

		int totalLinks = linkIndexMap.size();
		int[][] adjacency = new int[totalLinks][totalLinks];

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
	 * Gets the page rank map
	 * 
	 * @param adjacency The adjacency matrix
	 * @return The page rank map
	 */
	private HashMap<String, Double> getPageRankMap(int[][] adjacency) {

		return null;
	}

	/**
	 * Gets the page rank map
	 * 
	 * @param webInfoList The web info list
	 * @return The pagerank map
	 */
	private HashMap<String, Double> getPageRankMap(List<WebInfo> webInfoList) {

		HashMap<String, Integer> linkIndexMap = this.getLinkIndexMap(webInfoList);
		int[][] adjacency = this.prepareAdjacencyMatrix(webInfoList, linkIndexMap);

		// Removing the loops
		adjacency = this.removeLoops(adjacency);
		return this.getPageRankMap(adjacency);
	}

	/**
	 * Removes the loops
	 * 
	 * @param adjacencyMatrix The adjacency matrix
	 */
	private int[][] removeLoops(int[][] adjacencyMatrix) {

		LoopDetector loopDetector = new LoopDetector();
		List<Connection> loops = loopDetector.getLoops(adjacencyMatrix);
		for (Connection connection : loops) {
			adjacencyMatrix[connection.getSourceLinkIndex()][connection.getTargetLinkIndex()] = 0;
		}

		return adjacencyMatrix;
	}

	/**
	 * Reduces the web info list
	 */
	@Override
	public void reduce(String key, Iterator<WebInfo> values, OutputCollector<String, Double> output, Reporter reporter)
			throws IOException {

		List<WebInfo> webInfoList = new ArrayList<>();
		while (values.hasNext()) {
			webInfoList.add(values.next());
		}

		HashMap<String, Double> pagerankMap = this.getPageRankMap(webInfoList);
		for (Entry<String, Double> entry : pagerankMap.entrySet()) {
			output.collect(entry.getKey(), entry.getValue());
		}
	}
}
