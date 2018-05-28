package com.nasa;

import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Implementação utilizando spark mais java 8.
 * compilação e download via maven project.
 * Arquivo com respostas geradas na pasta doc/respostas-desafio-nasa.
 * @author rsfagundes
 *
 */
public class NasaAnalitics {

	private static String error404 = "404 -";
	private static Logger logger = Logger.getLogger(NasaAnalitics.class);

	public static void main(String[] args) {
		// Caminho do arquivo para leitura.
		String inputFile = null;
		if (0 > args.length) {
			inputFile = args[0];
			logger.info(inputFile);
		} else {
//			inputFile = "C:\\Users\\rsfagundes\\Downloads\\NASA_access_log_Aug95\\NASA_access_log_Aug95";
			inputFile = "C:\\Users\\rsfagundes\\Downloads\\NASA_access_log_Aug95\\NASA_access_log_Jul95";
			logger.info(inputFile);
		}

		SparkConf conf = new SparkConf().setAppName("nasa-analitics").setMaster("local[2]").set("spark.executor.memory", "1g");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> file = sc.textFile(inputFile);
		file.cache();
		file.count();
		JavaRDD<String> totalErrors404 = file.filter(object -> String.valueOf(object).contains(error404));

		List<String> dadosHost = file.collect();
		Set<String> hostUnicos = new HashSet<>();
		for (String linha : dadosHost) {
			String[] arrayValores = linha.split(" ");
			hostUnicos.add(arrayValores[0].toLowerCase().trim());
		}

		SortedMap<String, Integer> orderUrls = new TreeMap<String, Integer>();
		for (String linha : dadosHost) {
			String[] arrayValores = linha.split(" ");
			String data = arrayValores[0].toLowerCase().trim();
			if (arrayValores.length > 3) {
				String code = arrayValores[arrayValores.length - 2];
				if (code.equals("404")) {
					if (orderUrls.containsKey(data)) {
						orderUrls.put(data, orderUrls.get(data) + 1);
					} else {
						orderUrls.put(data, 1);
					}
				}
			}

		}

		logger.info("Os 5 URLs que mais causaram erro 404:");
		int count = 0;

		for (Entry<String, Integer> entry : orderUrls.entrySet()) {
			logger.info("URL:" + entry.toString());
			count++;
			if (count > 6) {
				break;
			}
		}

		List<String> dadosDia = totalErrors404.collect();
		SortedMap<String, Integer> diasCountError = new TreeMap<String, Integer>();

		for (String linha : dadosDia) {
			String[] arrayValores = linha.split(" ");
			String data = arrayValores[3].substring(1, 13);
			if (diasCountError.containsKey(data)) {
				diasCountError.put(data, diasCountError.get(data) + 1);
			} else {
				diasCountError.put(data, 1);
			}
		}
		logger.info("Quantidade de erros 404 por dia: ");
		for (Entry<String, Integer> entry : diasCountError.entrySet()) {
			logger.info("Data: " + entry.getKey() + " Total: " + entry.getValue());
		}

		JavaDoubleRDD bytes = file.mapToDouble(t -> {
			String[] arrayValores = t.split(" ");
			double s = 0;
			if (arrayValores.length > 3 && StringUtils.isNumeric(arrayValores[(arrayValores.length - 1)])) {
				s = Double.parseDouble(arrayValores[(arrayValores.length - 1)]);
			}
			return s + s;
		});
		java.math.BigDecimal totalBytes = new java.math.BigDecimal(bytes.sum());

		logger.info("O total de erros 404: " + totalErrors404.count());
		logger.info("O total de bytes retornados: " + totalBytes.toString());
		logger.info("Número de hosts únicos: " + hostUnicos.size());

	}
}
