package com.data.utils;

import java.util.ArrayList;
import java.util.List;

import com.data.models.Connection;

public class ReturnLinkDetector {

	private List<Connection> loops = new ArrayList<>();

	/**
	 * Detects the loops in adjacency matrix
	 * 
	 * @param currentLinkIndex The current link index
	 * @param adjacency        The adjacency matrix
	 * @param visited          The visited links array
	 */
	private void detect(int currentLinkIndex, int lastLinkIndex, int[][] adjacency, boolean[][] visited) {

		for (int i = 0; i < adjacency.length; i++) {
			if (adjacency[currentLinkIndex][i] == 1) {
				if (visited[currentLinkIndex][i]) {
					loops.add(new Connection(lastLinkIndex, currentLinkIndex));
					return;
				}
				visited[currentLinkIndex][i] = true;
				detect(i, currentLinkIndex, adjacency, visited);
			}
		}
	}

	/**
	 * Gets the loops from the adjacency matrix
	 * 
	 * @param adjacencyMatrix The adjacency matrix
	 * @return The loops
	 */
	public List<Connection> getLoops(int[][] adjacencyMatrix) {

		detect(0, -1, adjacencyMatrix, new boolean[adjacencyMatrix.length][adjacencyMatrix.length]);
		return loops;
	}
}
