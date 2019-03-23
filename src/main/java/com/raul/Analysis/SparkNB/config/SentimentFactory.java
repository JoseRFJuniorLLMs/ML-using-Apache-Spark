package com.raul.Analysis.SparkNB.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.raul.Analysis.SparkNB.processor.ISentimentsType;
import com.raul.Analysis.SparkNB.processor.NegativeSentProcessor;
import com.raul.Analysis.SparkNB.processor.PositiveSentProcessor;

@Component
public class SentimentFactory {

	@Autowired
	private PositiveSentProcessor positiveSentProcessor;

	@Autowired
	private NegativeSentProcessor negativeSentProcessor;

	public ISentimentsType get(String appName) {
		switch (appName.toUpperCase()) {
		case "POSITIVE":
			return positiveSentProcessor;
		case "NEGATIVE":
			return negativeSentProcessor;
		default:
			throw new IllegalArgumentException("No Driver Available for " + appName);

		}
	}

}
