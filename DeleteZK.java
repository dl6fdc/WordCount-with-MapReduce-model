import java.io.*;
import java.util.*;
import java.nio.file.*;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;

public class DeleteZK {
	public static void main(String args[]) {
		String host = "localhost";
		int port = 3922;
		ZooKeeper zk = null;
		final int SESSION_TIMEOUT = 10000;
		
		Random rand = new Random();
		host = "ece-acis-dc44" + (rand.nextInt(3) + 1) + ".acis.ufl.edu";
		String addr = host + ":" + port;
		try {
			zk = new ZooKeeper(addr, SESSION_TIMEOUT, null);
		} catch (IOException e) {
			System.out.println("Cannot create ZooKeeper Instance");
			System.out.println("Got an exception:" + e.getMessage());
		}
	
		try {
			ZKUtil.deleteRecursive(zk, "/words/440");
			ZKUtil.deleteRecursive(zk, "/words/441");
			ZKUtil.deleteRecursive(zk, "/words/442");
			ZKUtil.deleteRecursive(zk, "/words/443");
			ZKUtil.deleteRecursive(zk, "/words/444");
			ZKUtil.deleteRecursive(zk, "/words");
			ZKUtil.deleteRecursive(zk, "/output");
		} catch (Exception e) {
			System.out.println("error in deleting nodes.");
			System.out.println("Got an exception:" + e.getMessage());
		}
		
		try {
			zk.close();
		} catch (InterruptedException e) {
			System.out.println("Cannot close ZooKeeper Instance");
			System.out.println("Got an exception:" + e.getMessage());
		}
	}
}
