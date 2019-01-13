package org.diku.yuan.bds_project;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


/* If spark-testing-base dependency does not work for you, you can this local SharedSparkContext. */ 
public class SharedJavaSparkContextLocal {
	public static JavaSparkContext jsc;
	public static JavaSparkContext jsc() {
		if (jsc == null) {
			jsc = new JavaSparkContext(new SparkConf().setAppName("bds-project-test").setMaster("local[*]"));
			jsc.setLogLevel("ERROR");
		}
		return jsc;
	}
}
