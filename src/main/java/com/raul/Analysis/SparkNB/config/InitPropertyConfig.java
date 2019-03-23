package com.raul.Analysis.SparkNB.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.springframework.cloud.aws.core.io.s3.SimpleStorageResourceLoader;
import org.springframework.cloud.bootstrap.config.PropertySourceLocator;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.FileSystemResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InitPropertyConfig extends DefaultResourceLoader implements PropertySourceLocator {

	private ResourceLoader resourceLoader;

	public PropertySource<?> locate(Environment environment) {
		// TODO Auto-generated method stub

		String propertyUri = System.getenv("PROPERTIES_URI");

		System.out.println("PropertySource!!!");
		Map<String, Object> properties = null;

		if (propertyUri == null) {

			return new MapPropertySource("OverrideAppProperties", properties);
		}

		try {
			properties = readProperty(propertyUri);
			return new MapPropertySource("OverrideAppProperties", properties);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private Map<String, Object> readProperty(String propertyUri) throws Exception {
		// TODO Auto-generated method stub
		
		
		
		SimpleStorageResourceLoader simpleStorageResourceLoader = new SimpleStorageResourceLoader(
				AmazonS3ClientBuilder.standard().withRegion("ap-south-1").build(), new FileSystemResourceLoader());
		simpleStorageResourceLoader.afterPropertiesSet();
		resourceLoader = simpleStorageResourceLoader;
		Resource resource = resourceLoader.getResource(propertyUri);
		
		if (resource == null) {
			throw new IOException();
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(resource.getInputStream()));
		Properties properties = new Properties();
		properties.load(br);

		return properties.entrySet().stream()
				.collect(Collectors.toMap(a -> a.getKey().toString(), a -> a.getValue().toString()));
	}

}
