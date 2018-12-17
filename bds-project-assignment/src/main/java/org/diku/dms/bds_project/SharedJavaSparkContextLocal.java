package org.diku.dms.bds_project;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


/* If spark-testing-base dependency doeSharedJavaSparkContextLocas not work for you, you can this local SharedSparkContext. */ 
public class SharedJavaSparkContextLocal {
	public static JavaSparkContext jsc;
	public static JavaSparkContext jsc() {
		if (jsc == null) {
			SparkConf sparkConf = new SparkConf().setAppName("dbs-project").setMaster("local").set("spark.driver.allowMultipleContexts", "true");
			jsc = new JavaSparkContext(sparkConf);
		}
		return jsc;
	}
}
