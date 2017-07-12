package org.avm.researchgate.bigdata.textmining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;



public class Main {
	public static void main(String[] args) {
	    String logFile = "README.md"; // Should be some file on your system
	    SparkConf conf = new SparkConf().setAppName("HelloWorld Bigdata Textmining Application").setMaster("local[2]").set("spark.executor.memory","1g");;
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> logData = sc.textFile(logFile).cache();

	    
	    
	    long numAs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("a"); }
	    }).count();

	    long numBs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("b"); }
	    }).count();

	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	    
	    sc.stop();
	  }
}
