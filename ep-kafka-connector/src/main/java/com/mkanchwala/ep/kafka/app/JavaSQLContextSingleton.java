package com.mkanchwala.ep.kafka.app;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;

/** Lazily instantiated singleton instance of SQLContext */
class JavaSQLContextSingleton {
	static private transient SQLContext instance = null;

	static public SQLContext getInstance(SparkContext sparkContext) {
		if (instance == null) {
			instance = new SQLContext(sparkContext);
		}
		return instance;
	}
}