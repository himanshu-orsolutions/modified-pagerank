package com.data.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipExtractor {

	private ZipExtractor() {
		// Utility class
	}

	/**
	 * Extracts the zip file
	 * 
	 * @param zipFilePath The zip file path
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void extract(Path zipFilePath) throws IOException {

		try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFilePath.toFile()))) {
			byte[] buffer = new byte[1024];
			ZipEntry zipEntry = zipInputStream.getNextEntry();
			while (zipEntry != null) {
				if (zipEntry.isDirectory()) {
					Path directoryPath = Paths.get(zipEntry.getName());
					if (!directoryPath.toFile().exists()) {
						Files.createDirectory(directoryPath);
					}
				} else {
					try (FileOutputStream outputStream = new FileOutputStream(zipEntry.getName())) {
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
}
