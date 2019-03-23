package com.raul.Analysis.SparkNB.Analyzer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.abs;

public class SentimentsAnalyzer implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2707589082909548399L;

	public String analyzer(Dataset<Row> processedPosSentiments, Dataset<Row> processedNegSentiments,
			Dataset<String> analysisDataset, Dataset<String> stopWordsDataSet) {

		List<String> stopList = stopWordsDataSet.collectAsList();

		Dataset<String> words = analysisDataset.flatMap(s -> {
			return Arrays.asList(s.toLowerCase().split(" ")).iterator();
		}, Encoders.STRING()).filter(s -> !s.isEmpty()).filter(s -> s.length() > 2).filter(s -> s.matches("[A-Za-z]*"))
				.filter(s -> !stopList.contains(s)).coalesce(1);

		Dataset<Row> analysisTable = words.groupBy("value").count().toDF("word", "count");

		Dataset<Row> posAnalysisTable = analysisTable
				.join(processedPosSentiments, analysisTable.col("word").equalTo(processedPosSentiments.col("word")))
				.select(processedPosSentiments.col("word"), processedPosSentiments.col("probability"),
						round(analysisTable.col("count").multiply(processedPosSentiments.col("probability")), 5)
								.as("CountWithAnalysisData"));

		Double a = 1.00;

		posAnalysisTable = posAnalysisTable.withColumn("CountWithAnalysisData",
				when(posAnalysisTable.col("CountWithAnalysisData").isNull()
						.or(posAnalysisTable.col("CountWithAnalysisData").like("%E%")),
						abs(posAnalysisTable.col("probability").minus(a)))
								.otherwise(posAnalysisTable.col("CountWithAnalysisData")));

		posAnalysisTable = posAnalysisTable.sort(functions.desc("CountWithAnalysisData"));

		Dataset<Row> negAnalysisTable = analysisTable
				.join(processedNegSentiments, analysisTable.col("word").equalTo(processedNegSentiments.col("word")))
				.select(processedNegSentiments.col("word"), processedNegSentiments.col("probability"),
						round(analysisTable.col("count").multiply(processedNegSentiments.col("probability")), 5)
								.as("CountWithAnalysisData"));

		negAnalysisTable = negAnalysisTable.withColumn("CountWithAnalysisData",
				when(negAnalysisTable.col("CountWithAnalysisData").isNull()
						.or(negAnalysisTable.col("CountWithAnalysisData").like("%E%")),
						abs(negAnalysisTable.col("probability").minus(a)))
								.otherwise(negAnalysisTable.col("CountWithAnalysisData")));

		negAnalysisTable = negAnalysisTable.sort(functions.desc("CountWithAnalysisData"));

		Optional<Double> posProbability = Optional.of(new Double(0));
		Optional<Double> negProbability = Optional.of(new Double(0));

		posProbability = posAnalysisTable.select(posAnalysisTable.col("CountWithAnalysisData"))
				.filter(row -> !row.anyNull()).as(Encoders.DOUBLE()).collectAsList().stream()
				.reduce((a1, b1) -> a1 + b1);

		negProbability = negAnalysisTable.select(negAnalysisTable.col("CountWithAnalysisData"))
				.filter(row -> !row.anyNull()).as(Encoders.DOUBLE()).collectAsList().stream()
				.reduce((a1, b1) -> a1 + b1);

		if (!posProbability.isPresent()) {
			posProbability = Optional.of(new Double(0));
		}
		if (!negProbability.isPresent()) {
			negProbability = Optional.of(new Double(0));
		}

		System.out.println("Cummulative Positive Probability " + posProbability.get());

		System.out.println("Cummulative Negative Probability " + negProbability.get());

		if (posProbability.get().compareTo(negProbability.get()) == 1) {
			System.out.println("Dataset is Positive");
			return "Data is Positive";
		}
		if (posProbability.get().compareTo(negProbability.get()) == -1) {
			System.out.println("Dataset is Negative");
			return "Data is Negative";
		}

		return "";
	}
}
