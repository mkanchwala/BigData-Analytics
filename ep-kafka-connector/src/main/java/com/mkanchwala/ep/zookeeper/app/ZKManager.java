package com.mkanchwala.ep.zookeeper.app;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import kafka.common.TopicAndPartition;

public class ZKManager {

	private static ZooKeeper zk;
	final CountDownLatch connectedSignal = new CountDownLatch(1);

	public ZKManager(String node) {
		try {
			zk = this.connect(node);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// Method to connect zookeeper ensemble.
	public ZooKeeper connect(String host) throws IOException, InterruptedException {

		zk = new ZooKeeper(host, 5000, new Watcher() {

			public void process(WatchedEvent we) {

				if (we.getState() == KeeperState.SyncConnected) {
					connectedSignal.countDown();
				}
			}
		});

		connectedSignal.await();
		return zk;
	}

	// Method to disconnect from zookeeper server
	public void close() throws InterruptedException {
		zk.close();
	}

	public static Stat znode_exists(String path) throws KeeperException, InterruptedException {
		return zk.exists(path, true);
	}

	// Method to create znode in zookeeper ensemble
	public static void create(String path, byte[] data) throws KeeperException, InterruptedException {
		zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	// Method to update the data in a znode. Similar to getData but without
	// watcher.
	public static void update(String path, byte[] data) throws KeeperException, InterruptedException {
		zk.setData(path, data, zk.exists(path, true).getVersion());
	}

	// Method to check existence of znode and its status, if znode is available.
	public static void delete(String path) throws KeeperException, InterruptedException {
		zk.delete(path, zk.exists(path, true).getVersion());
	}

	// Method to check existence of znode and its status, if znode is available.
	public static void findOffsetRange(final String path)
			throws KeeperException, InterruptedException, UnsupportedEncodingException {
		final CountDownLatch connectedSignal = new CountDownLatch(1);
		Stat stat = znode_exists(path);
		if (stat != null) {
			byte[] b = zk.getData(path, new Watcher() {

				public void process(WatchedEvent we) {

					if (we.getType() == Event.EventType.None) {
						switch (we.getState()) {
						case Expired:
							connectedSignal.countDown();
							break;
						}
					} else {
						try {
							byte[] bn = zk.getData(path, false, null);
							String data = new String(bn, "UTF-8");
							System.out.println(data);
							connectedSignal.countDown();
						} catch (Exception ex) {
							System.out.println(ex.getMessage());
						}
					}
				}
			}, null);

			String data = new String(b, "UTF-8");
			System.out.println(data);
			connectedSignal.await();

		} else {
			System.out.println(path + " does not exists");
		}
	}

	// Method to check existence of znode and its status, if znode is available.
	public static Map<TopicAndPartition, Long> getOffsetRange(final String path) throws KeeperException, InterruptedException, UnsupportedEncodingException {

		Map<TopicAndPartition, Long> startOffsetsMap = new HashMap<TopicAndPartition, Long>();
		startOffsetsMap.put(new TopicAndPartition("", 0), 22L);

		final CountDownLatch connectedSignal = new CountDownLatch(1);
		Stat stat = znode_exists(path);
		if (stat != null) {
			byte[] b = zk.getData(path, new Watcher() {

				public void process(WatchedEvent we) {

					if (we.getType() == Event.EventType.None) {
						switch (we.getState()) {
						case Expired:
							connectedSignal.countDown();
							break;
						}
					} else {
						try {
							byte[] bn = zk.getData(path, false, null);
							String data = new String(bn, "UTF-8");
							System.out.println(data);
							connectedSignal.countDown();
						} catch (Exception ex) {
							System.out.println(ex.getMessage());
						}
					}
				}
			}, null);

			String data = new String(b, "UTF-8");
			System.out.println(data);
			connectedSignal.await();

		} else {
			System.out.println(path + " does not exists");
		}

		return null;
	}
}
