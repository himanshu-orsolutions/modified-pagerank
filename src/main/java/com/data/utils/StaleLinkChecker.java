package com.data.utils;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;

public class StaleLinkChecker {

	private static HashSet<String> staleLinks = new HashSet<>();
	private static HashSet<String> activeLinks = new HashSet<>();

	/**
	 * Checks if the URL is stale or not
	 * 
	 * @param url The URL
	 * @return If the link is stale or not
	 */
	public static boolean check(String url) {

		if (staleLinks.contains(url)) {
			return true;
		} else if (activeLinks.contains(url)) {
			return false;
		} else {
			try {
				URL httpURL = new URL(url);
				httpURL.openConnection().connect();
			} catch (IOException ioException) {
				return true;
			}
			return false;
		}
	}
}
