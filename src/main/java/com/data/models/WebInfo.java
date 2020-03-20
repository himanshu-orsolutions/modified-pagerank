package com.data.models;

import java.util.List;

public class WebInfo {
	private String hadoopSourceURL;
	private List<String> hadoopTargetURLs;

	public WebInfo(String hadoopSourceURL, List<String> hadoopTargetURLs) {
		super();
		this.hadoopSourceURL = hadoopSourceURL;
		this.hadoopTargetURLs = hadoopTargetURLs;
	}

	public String getHadoopSourceURL() {
		return hadoopSourceURL;
	}

	public void setHadoopSourceURL(String hadoopSourceURL) {
		this.hadoopSourceURL = hadoopSourceURL;
	}

	public List<String> getHadoopTargetURLs() {
		return hadoopTargetURLs;
	}

	public void setHadoopTargetURLs(List<String> hadoopTargetURLs) {
		this.hadoopTargetURLs = hadoopTargetURLs;
	}
}
