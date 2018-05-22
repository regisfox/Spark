package com.nasa;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class NasaAnalitics {

	private static Logger logger = Logger.getLogger(NasaAnalitics.class);

	public static void main(String[] args) {

		String inputFile = "C:\\Users\\rsfagundes\\Downloads\\NASA_access_log_Aug95\\NASA_access_log_Aug95";

		SparkConf conf = new SparkConf().setAppName("nasa-analitics").setMaster("local[2]").set("spark.executor.memory", "1g");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> file = sc.textFile(inputFile);
		
		JavaRDD<String> hostUnicos = file.distinct().filter(object -> String.valueOf(object).contains("404 -"));
		JavaRDD<String> totalError404 = file.filter(object -> String.valueOf(object).contains("404 -"));
		
		JavaRDD<String> mais5UrlsError404 = file.distinct().filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String v1) throws Exception {
				return v1.contains("404 - ");
			}
		});
		JavaRDD<String> pythonLines = file.filter(line -> line.contains("Python"));
		JavaRDD<String> totaBytes = file.filter(object -> String.valueOf(object).contains("404 -"));
		
		long numHostUnicos = hostUnicos.count();
		long numTotalErrors404 = totalError404.count();
		long numMais5UrlsError404 = mais5UrlsError404.count();
		long numTotaBytes = totaBytes.count();
		logger.info("Número de hosts únicos: " + numHostUnicos);
		logger.info("O total de erros 404: " + numTotalErrors404);
		logger.info("Os 5 URLs que mais causaram erro 404: " + numMais5UrlsError404);
		logger.info("O total de bytes retornados: " + numTotaBytes);

	}
}
