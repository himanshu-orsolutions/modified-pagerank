package com.data.utils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.file.Paths;

public class ZipFileDownloader {

	private ZipFileDownloader() {
		// Utility class
	}

	/**
	 * Gets the file name
	 * 
	 * @param url The zip file URL
	 * @return The file name
	 */
	private static String getFileName(URL url) {
		return url.getPath().substring(url.getPath().lastIndexOf("/") + 1);
	}

	/**
	 * Downloads the zip file
	 * 
	 * @param url The URL
	 * @return The zip file name
	 * @throws IOException
	 */
	public static String download(URL url) throws IOException {

		String zipFileName = getFileName(url);
		if (zipFileName.endsWith(".zip")) {
			if (!Paths.get(zipFileName).toFile().exists()) {
				try (FileOutputStream outputStream = new FileOutputStream(zipFileName)) {
					outputStream.getChannel().transferFrom(Channels.newChannel(url.openStream()), 0, Long.MAX_VALUE);
				}
			}
			return zipFileName;
		}
		return "";
	}
}
