package com.mkanchwala.ep.kafka.app;

/** Java Bean class to be used with the example JavaSqlNetworkWordCount. */
public class JavaRecord implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8301908697897785906L;
	private String word;

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}
}