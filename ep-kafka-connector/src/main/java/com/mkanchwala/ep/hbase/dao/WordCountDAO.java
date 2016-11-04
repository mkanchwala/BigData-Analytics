package com.mkanchwala.ep.hbase.dao;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;

import scala.Tuple2;

public class WordCountDAO implements Serializable {

	private static final long serialVersionUID = -3816014552834940186L;

	public void writeRowNewHadoopAPI(JavaRDD<Row> records, Configuration conf) {

		JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = records
				.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
					private static final long serialVersionUID = 5006255093946286577L;

					@SuppressWarnings("deprecation")
					@Override
					public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
						System.out.println("Row : " + row.toString());
						Put put = new Put(Bytes.toBytes("rowkey11"));
						put.add(Bytes.toBytes("w1"), Bytes.toBytes("Z"), Bytes.toBytes("value3"));
						return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
					}

				});
		
		if(!hbasePuts.isEmpty())
			hbasePuts.saveAsNewAPIHadoopDataset(conf);
	}
}
