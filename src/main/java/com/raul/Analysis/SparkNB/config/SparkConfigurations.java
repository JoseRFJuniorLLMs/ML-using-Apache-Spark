package com.raul.Analysis.SparkNB.config;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

@Component
@EnableAutoConfiguration
@ConfigurationProperties(prefix="spark")
@Data
public class SparkConfigurations {

	private Map<String,String> spConfig = new HashMap<>();

	public Map<String,String> getSpConfig() {
		return spConfig;
	}

	public void setSpConfig(Map<String,String> spConfig) {
		this.spConfig = spConfig;
	}
}
