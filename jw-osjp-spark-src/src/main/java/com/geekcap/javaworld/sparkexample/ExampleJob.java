package com.geekcap.javaworld.sparkexample;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.base.Optional;

import scala.Tuple2;

public class ExampleJob {

	JavaSparkContext sc;
	
	PairFunction<Tuple2<Integer,Optional<String>>,Integer,String> PAIR_FUCTION = new PairFunction<Tuple2<Integer,Optional<String>>,Integer,String>() {

		@Override
		public Tuple2<Integer, String> call(Tuple2<Integer, Optional<String>> a) throws Exception {
			return new Tuple2<Integer, String>(a._1, a._2.get());
		}
	};
	
	public ExampleJob(JavaSparkContext sc) {
		this.sc = sc;
	}
	
	public static void main(String[] args) {
		
		JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setAppName("SparkJoins").setMaster("local"));
		ExampleJob exampleJob = new ExampleJob(jsc);
		String users_path = "/sparkdata/users.txt";
		String transactions_path = "/sparkdata/transactions.txt";
		JavaPairRDD<String, String> outpud_rdd = exampleJob.run(transactions_path,users_path);
		outpud_rdd.saveAsHadoopFile("/sparkdata/users_transactions_output", String.class, String.class, TextOutputFormat.class);
	}



	private JavaPairRDD<String, String> run(String tr, String usr) {
		JavaRDD<String> tr_input = sc.textFile(tr);
		JavaPairRDD<Integer, Integer> tr_pair = tr_input.mapToPair(new PairFunction<String, Integer,Integer>() {
			@Override
			public Tuple2<Integer, Integer> call(String tr_record) throws Exception {
				String[] trSplit = tr_record.split("\t");
				return new Tuple2<Integer, Integer>(Integer.valueOf(trSplit[2]), Integer.valueOf(trSplit[1]));
			}
		});
		
		
		JavaRDD<String> usr_input = sc.textFile(usr);
		JavaPairRDD<Integer, String> usr_pair = usr_input.mapToPair(new PairFunction<String,Integer,String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<Integer, String> call(String tr_record) throws Exception {
				String[] split = tr_record.split("/t");
				return new Tuple2<Integer, String>(Integer.valueOf(split[0]),split[3]);
			}
		});
			Map<Integer, Object> countData = countData(modifyData(joinData(tr_pair,usr_pair)));
			
			List<Tuple2<String, String>> output = new ArrayList<>();
			for (Entry<Integer, Object> e : countData.entrySet()) {
				output.add(new Tuple2<String, String>(e.getKey().toString(), String.valueOf((long)e.getValue())));
			}
			return null;
	}

	private Map<Integer, Object> countData(JavaPairRDD<Integer, String> modifyData) {
		return modifyData.countByKey();
	}

	private JavaPairRDD<Integer, String> modifyData(JavaRDD<Tuple2<Integer, Optional<String>>> joinData) {
		return joinData.mapToPair(PAIR_FUCTION);
	}

	private JavaRDD<Tuple2<Integer, Optional<String>>> joinData(JavaPairRDD<Integer, Integer> tr_pair, JavaPairRDD<Integer, String> usr_pair) {
		JavaRDD<Tuple2<Integer, Optional<String>>> distinct = tr_pair.leftOuterJoin(usr_pair).values().distinct();
		return distinct;
	}



	
}
