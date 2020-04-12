package com.data.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.data.models.Connection;
import com.data.models.WebInfo;
import com.data.utils.LoopDetector;
import com.data.utils.StaleLinkChecker;
import com.google.gson.Gson;

public class LinkReducer extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {

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

			if (!StaleLinkChecker.check(sourceURL) && !linkIndexMap.containsKey(sourceURL)) {
				linkIndexMap.put(sourceURL, index++);
			}
			for (String targetURL : targetURLs) {
				if (!StaleLinkChecker.check(targetURL) && !linkIndexMap.containsKey(targetURL)) {
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
	 * @param linkIndexMap The link index map
	 * @param adjacency    The adjacency matrix
	 * @return The page rank map
	 */
	private HashMap<String, Double> getPageRankMap(HashMap<String, Integer> linkIndexMap, int[][] adjacency) {

		int totalLinks = linkIndexMap.size();
		double[] pageRanks = new double[totalLinks];
		for (int i = 0; i < totalLinks; i++) {
			pageRanks[i] = 1.0d;
		}

		// Calculating the page ranks
		for (int i = 0; i < totalLinks; i++) {
			int totalOutgoings = 0;
			for (int j = 0; j < totalLinks; j++) {
				if (adjacency[i][j] == 1) {
					totalOutgoings++;
				}
			}

			if (totalOutgoings > 0) {
				for (int j = 0; j < totalLinks; j++) {
					if (adjacency[i][j] == 1) {
						pageRanks[j] += pageRanks[i] / totalOutgoings;
					}
				}
			}
		}

		// Preparing the map
		HashMap<String, Double> pageRankMap = new HashMap<>();
		for (Entry<String, Integer> entry : linkIndexMap.entrySet()) {
			pageRankMap.put(entry.getKey(), pageRanks[entry.getValue()]);
		}

		return pageRankMap;
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
		return this.getPageRankMap(linkIndexMap, adjacency);
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
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {

		List<WebInfo> webInfoList = new ArrayList<>();
		while (values.hasNext()) {
			webInfoList.add(new Gson().fromJson(values.next().toString(), WebInfo.class));
		}

		HashMap<String, Double> pagerankMap = this.getPageRankMap(webInfoList);
		for (Entry<String, Double> entry : pagerankMap.entrySet()) {
//			output.collect(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
		}
	}
}
