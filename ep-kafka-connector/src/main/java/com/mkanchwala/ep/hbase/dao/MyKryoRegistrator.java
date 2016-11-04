package com.mkanchwala.ep.hbase.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;

public class MyKryoRegistrator implements KryoRegistrator {

	@Override
	public void registerClasses(Kryo kryo) {
		kryo.register(Configuration.class);

	}

}