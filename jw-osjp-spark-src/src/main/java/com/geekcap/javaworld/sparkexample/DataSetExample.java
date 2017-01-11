package com.geekcap.javaworld.sparkexample;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;


public class DataSetExample {
	
	public static void main(String[] args) throws Exception {
		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("DataSetExample").setMaster("local"));
		SQLContext sqlContext = new SQLContext(sc);
		List data = getEmployees();
		Dataset dataset = sqlContext.createDataset(data, Encoders.bean(Employee.class));
		Dataset filter = dataset.filter("age > 100");
		filter.show();
}

	private static List<Employee> getEmployees() {
		List<Employee> emps = new ArrayList<>();
		for (int i = 0; i < 100000; i++) {
			Employee e = new Employee();
			e.setAge(i%100);
			e.setId(i);
			e.setName("Emp"+i);
			e.setSalary(e.getAge()*2);
			Date date = new Date();
			date.setDate(i);
			e.setDob(date);
			emps.add(e);
		}
		return emps;
	}
	}
