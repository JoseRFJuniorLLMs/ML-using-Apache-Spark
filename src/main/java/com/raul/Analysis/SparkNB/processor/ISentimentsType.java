package com.raul.Analysis.SparkNB.processor;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface ISentimentsType {

	public Dataset<Row> toWordProb(Dataset<String> dataset, Dataset<String> stopWords) throws IOException;
}
