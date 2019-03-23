package com.raul.Analysis.SparkNB.config;

import org.springframework.context.annotation.Configuration;

import com.raul.Analysis.SparkNB.processor.NegativeSentProcessor;
import com.raul.Analysis.SparkNB.processor.PositiveSentProcessor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;;

@Configuration
public class BeanConfig {
	
	@Bean
	public SparkConf sparkConf(SparkConfigurations sparkConfigurations)
	{
		SparkConf sparkConf = new SparkConf();
		sparkConfigurations.getSpConfig().forEach(sparkConf::set);
		return sparkConf;
	}
	
	@Bean
	public SparkSession sparkSession(SparkConf sparkConf)
	{
		return SparkSession.builder().config(sparkConf).getOrCreate();
	}
	
	@Bean
	public org.apache.hadoop.conf.Configuration conf(SparkSession sparkSession)
	{
		return sparkSession.sparkContext().hadoopConfiguration();
	}
	
	@Bean
	public SentimentFactory sentimentFactory()
	{
		return new SentimentFactory();
	}

	@Bean
	public PositiveSentProcessor positiveSentProcessor()
	{
		return new PositiveSentProcessor();
	}
	
	@Bean
	public NegativeSentProcessor negativeSentProcessor()
	{
		return new NegativeSentProcessor();
	}
}
