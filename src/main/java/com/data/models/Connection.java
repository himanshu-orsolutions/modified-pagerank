package com.data.models;

public class Connection {

	private int sourceLinkIndex;
	private int targetLinkIndex;

	public Connection(int sourceLinkIndex, int targetLinkIndex) {
		super();
		this.sourceLinkIndex = sourceLinkIndex;
		this.targetLinkIndex = targetLinkIndex;
	}

	public int getSourceLinkIndex() {
		return sourceLinkIndex;
	}

	public void setSourceLinkIndex(int sourceLinkIndex) {
		this.sourceLinkIndex = sourceLinkIndex;
	}

	public int getTargetLinkIndex() {
		return targetLinkIndex;
	}

	public void setTargetLinkIndex(int targetLinkIndex) {
		this.targetLinkIndex = targetLinkIndex;
	}
}
