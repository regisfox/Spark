package com.nasa;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class NasaAnalitics {

	private static String error404 = "404 -";
	private static Logger logger = Logger.getLogger(NasaAnalitics.class);

	public static void main(String[] args) {
		// Caminho do arquivo para leitura.
		String inputFile = "C:\\Users\\rsfagundes\\Downloads\\NASA_access_log_Aug95\\NASA_access_log_Aug95";
		// String inputFile = args[0];

		SparkConf conf = new SparkConf().setAppName("nasa-analitics").setMaster("local[2]").set("spark.executor.memory", "1g");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> file = sc.textFile(inputFile);

		JavaRDD<String> totalErrors404 = file.filter(object -> String.valueOf(object).contains(error404));

		List<String> dadosHost = file.collect();
		Set<String> hostUnicos = new HashSet<>();
		for (String linha : dadosHost) {
			String[] arrayValores = linha.split(" ");
			hostUnicos.add(arrayValores[0]);
		}

		List<String> dadosDia = file.collect();
		for (String linha : dadosDia) {
			String[] arrayValores = linha.split(" ");
			if (arrayValores.length > 3) {
			String data = arrayValores[arrayValores.length-2];
				if (data.contains("404")) {
					System.out.println(data);	
				}
			}
		}

//		Set<String> dias = new HashSet<>();
//		for (String linha : dadosDia) {
//			String[] arrayValores = linha.split(" ");
//			if (arrayValores.length > 3) {
//				String data = arrayValores[3].substring(1, 13);
//				dias.add(data);
//			}
//		}

		JavaDoubleRDD bytes = file.mapToDouble(t -> {
			String[] arrayValores = t.split(" ");
			double s = 0;
			if (StringUtils.isNumeric(arrayValores[(arrayValores.length - 1)])) {
				s = Double.parseDouble(arrayValores[(arrayValores.length - 1)]);
			}
			return s + s;
		});
		java.math.BigDecimal totalBytes = new java.math.BigDecimal(bytes.sum());

		logger.info("O total de erros 404: " + totalErrors404.count());
		logger.info("Os 5 URLs que mais causaram erro 404: " + 0);
		logger.info("Quantidade de erros 404 por dia: " + 0);
		logger.info("O total de bytes retornados: " + totalBytes.toString());
		logger.info("Número de hosts únicos: " + hostUnicos.size());

	}
	
	public class DataControle{
		private String data;
		private long contador;
		public String getData() {
			return data;
		}
		public void setData(String data) {
			this.data = data;
		}
		public long getContador() {
			return contador;
		}
		public void setContador(long contador) {
			this.contador = contador;
		}
	}
}
