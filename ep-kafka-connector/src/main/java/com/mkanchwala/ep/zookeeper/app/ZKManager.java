package com.mkanchwala.ep.zookeeper.app;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import kafka.common.TopicAndPartition;

/**
 * @author murtaza.kanchwala
 * 
 * This class is useful for Zookeeper ZNode related CRUD operations
 * 
 * To use create a Global variable for it and initialize in its Startup or init method.
 * 
 * In Class variables:-
 * 
 * 		private static ZKManager zkClient;
 * 
 * In Constructor:- 
 * 
 * ABCProcessor(){
 * 		zkClient = new ZKManager(ConfigurationManager.getGeneralProprty(Constant.ZOOKEEPER_HOST));
 * }
 * 
 * Usage:- See below methods for usage. 
 * 
 * For Ex:- zkClient.saveOffset(final OffsetRange[] offsets, final String zkNode);
 *
 */
public class ZKManager {

	private static ZooKeeper zk;
	private static Logger logger = Logger.getLogger(ZKManager.class);
	
	public ZKManager(String node) {
		try {
			zk = this.connect(node);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Method to connect zookeeper ensemble.
	 * 
	 * @param host
	 * @return ZooKeeper
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public ZooKeeper connect(String host) throws IOException, InterruptedException {

		zk = new ZooKeeper(host, 5000, new Watcher() {

			public void process(WatchedEvent we) {

			}
		});

		return zk;
	}

	/**
	 * Method to disconnect from zookeeper server
	 * 
	 * @throws InterruptedException
	 */
	public void close() throws InterruptedException {
		zk.close();
	}

	/**
	 * Method to check if Znode Exists or not.
	 * 
	 * @param path
	 * @return Stat
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public Stat znode_exists(String path) throws KeeperException, InterruptedException {
		return zk.exists(path, true);
	}

	/**
	 * Method to create znode in zookeeper ensemble
	 * 
	 * @param path
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void create(String path, byte[] data) throws KeeperException, InterruptedException {
		zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}
	
	/**
	 * Method to update the data in a znode. Similar to getData but without watcher.
	 * 
	 * @param path
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void update(String path, byte[] data) throws KeeperException, InterruptedException {
		zk.setData(path, data, zk.exists(path, true).getVersion());
	}

	/**
	 * Method to check existence of znode and its status, if znode is available.
	 * 
	 * @param path
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void delete(String path) throws KeeperException, InterruptedException {
		zk.delete(path, zk.exists(path, true).getVersion());
	}

	/**
	 * Method to check existence of znode and its status, if znode is available.
	 * 
	 * @param path
	 * @return Map<TopicAndPartition, Long>
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws UnsupportedEncodingException
	 */
	public Map<TopicAndPartition, Long> findOffsetRange(final String path) throws KeeperException, InterruptedException, UnsupportedEncodingException {
		Map<TopicAndPartition, Long> startOffsetsMap = new HashMap<TopicAndPartition, Long>();
		Stat stat = znode_exists(path);

		if (stat != null) {

			List<String> children = zk.getChildren(path, false);
			for (int i = 0; i < children.size(); i++) {
				System.out.println(children.get(i)); 

				byte[] b = zk.getData(path + "/" + children.get(i), new Watcher() {

					@SuppressWarnings("incomplete-switch")
					public void process(WatchedEvent we) {

						if (we.getType() == Event.EventType.None) {
							switch (we.getState()) {
							case Expired:
								break;
							}

						} else {
							try {
								byte[] bn = zk.getData(path, false, null);
								String data = new String(bn, "UTF-8");
								System.out.println(data);

							} catch (Exception ex) {
								System.out.println(ex.getMessage());
							}
						}
					}
				}, null);

				String data = new String(b, "UTF-8");
				System.out.println(data);
				startOffsetsMap.put(
						new TopicAndPartition(data.split(";")[0].split("=")[1],
								Integer.parseInt(data.split(";")[1].split("=")[1])),
						Long.parseLong(data.split(";")[2].split("=")[1]));

			}
		} else {
			System.out.println("Node does not exists");
		}
		System.out.println(startOffsetsMap.size());
		return startOffsetsMap;
	}
	
	/**
	 * Method to save offset details and partition details topic wise.
	 * 
	 * @param offsets
	 * @param zkNode
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void saveOffset(final OffsetRange[] offsets, final String zkNode) throws KeeperException, InterruptedException {

		for (OffsetRange o : offsets) {
			String stats = "topic=" + o.topic() + ";partition=" + o.partition() + ";fromOffset=" + o.fromOffset()
					+ ";untilOffset=" + o.untilOffset();
			logger.debug(stats);
			update(zkNode + "/" + o.topic(), stats.getBytes());
		}
	}
}
