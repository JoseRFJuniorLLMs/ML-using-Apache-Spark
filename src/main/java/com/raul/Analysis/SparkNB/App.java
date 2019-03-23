package com.raul.Analysis.SparkNB;

import java.io.File;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.raul.Analysis.SparkNB.Analyzer.SentimentsAnalyzer;
import com.raul.Analysis.SparkNB.config.SentimentFactory;
import com.raul.Analysis.SparkNB.processor.ISentimentsType;

@SpringBootApplication
public class App implements CommandLineRunner {

	String filePath;

	@Autowired
	private SparkSession sparkSession;

	@Autowired
	private SentimentFactory sentimentFactory;

	@Autowired
	private SentimentsAnalyzer sentimentsAnalyzer;

	String posdb = "file:///";
	String negdb = "file:///";
	String analysisdb = "file:///";

	String stopWords = "file:///";

	public static void main(String[] args) {
		System.out.println("Starting!!!");
		SpringApplication.run(App.class, args);
		System.out.println("Ending!!!");

	}

	public void run(String... arg0) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Inside Run!!!");

		sparkSession.sparkContext().conf().setAppName("RaulSparkTesting");
		Dataset<String> stopWordsDataset = sparkSession.read().textFile(stopWords).as(Encoders.STRING());

		Dataset<String> posDataset = sparkSession.read().textFile(posdb).as(Encoders.STRING());
		ISentimentsType driver = sentimentFactory.get("positive");

		Dataset<Row> processedPositiveSentiments = driver.toWordProb(posDataset, stopWordsDataset);
		processedPositiveSentiments.persist();

		Dataset<String> negDataset = sparkSession.read().textFile(negdb).as(Encoders.STRING());
		driver = sentimentFactory.get("negative");
		Dataset<Row> processedNegativeSentiments = driver.toWordProb(negDataset, stopWordsDataset);
		processedNegativeSentiments.persist();

		ArrayList<String> score = new ArrayList<>();
		File analysisDir = new File(analysisdb);
		File[] allContents = analysisDir.listFiles();
		for (File file : allContents) {
			System.out.println("Analysing sentiments for file  " + file.getAbsolutePath());

			Dataset<String> analysisDataset = sparkSession.read().textFile("file:///" + file.getAbsolutePath())
					.as(Encoders.STRING());
			score.add(sentimentsAnalyzer.analyzer(processedPositiveSentiments, processedNegativeSentiments,
					analysisDataset, stopWordsDataset));
		}

		int posC = 0, negC = 0, neutralC = 0;

		for (int i = 0; i < score.size(); i++) {
			if (score.get(i).contains("NEGATIVE")) {
				negC++;
			}
			if (score.get(i).contains("POSITIVE")) {
				posC++;
			}
			if (score.get(i).equals("")) {
				neutralC++;
			}
		}

		System.out.println("Positive count " + posC + "\n Negative Count " + negC + "\n Neutral Count " + neutralC);

	}

}
