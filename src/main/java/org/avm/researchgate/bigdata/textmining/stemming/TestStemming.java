package org.avm.researchgate.bigdata.textmining.stemming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.spanishStemmer;

/**
 * POC of Stemming
 * 
 * @author alberto
 *
 */
public class TestStemming {

	private static final String getStem(String term) {
		final SnowballStemmer snowballStemmer = new spanishStemmer();
		snowballStemmer.setCurrent(term);
		snowballStemmer.stem();
		return snowballStemmer.getCurrent();

	}

	public static void main(String[] args) {

		final String logFile = "sample.txt"; // Should be some file on your
												// system
		final SparkConf conf = new SparkConf().setAppName("Stemming Bigdata Application")
				.setMaster("local[2]").set("spark.executor.memory", "1g");
		;
		final JavaSparkContext sc = new JavaSparkContext(conf);
		final JavaRDD<String> textData = sc.textFile(logFile).cache();

		final JavaRDD<String> result = textData.flatMap(s -> Arrays.asList(s.split("[ —\\.,]+")).iterator())
				.map(new Function<String, String>() {
					@Override
					public String call(String s) throws Exception {
						return getStem(s);
					}
				});

		// System.out.println(result.collect());

		sc.stop();
		sc.close();
	}

}
