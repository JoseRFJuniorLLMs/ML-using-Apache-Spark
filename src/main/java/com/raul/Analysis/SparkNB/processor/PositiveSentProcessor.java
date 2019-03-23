package com.raul.Analysis.SparkNB.processor;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class PositiveSentProcessor implements ISentimentsType, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8962625017489881635L;

	@Override
	public Dataset<Row> toWordProb(Dataset<String> dataset, Dataset<String> stopWords) throws IOException {
		// TODO Auto-generated method stub

		List<String> stopList = stopWords.collectAsList();
		Dataset<String> words = dataset.flatMap(s -> {
			return Arrays.asList(s.toLowerCase().split(" ")).iterator();
		}, Encoders.STRING()).filter(s -> !s.isEmpty()).filter(s -> s.length() > 2).filter(s -> s.matches("[A-Za-z]*"))
				.filter(s -> !stopList.contains(s)).coalesce(1);

		Integer tot = (int) words.count();
		System.out.println("Total Count " + tot);
		Dataset<Row> reduced = words.groupBy("value").count().toDF("word","count");;

		Dataset<Row> reducedWithProbabilty = reduced.withColumn("probability",
				reduced.col("count").divide(tot).cast(DataTypes.createDecimalType(20, 5)));

		reducedWithProbabilty = reducedWithProbabilty
				.filter(reducedWithProbabilty.col("probability").notEqual("0.00000")).sort(functions.desc("count"));

		return reducedWithProbabilty;
	}

}
