import java.io.*;
import java.util.*;
import java.nio.file.*;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;

public class WordCount {
	private static int pid;
	private static String fileFolder;
	private ZooKeeper zk;
	private static final int SESSION_TIMEOUT = 10000;
	private ArrayList<String> files0 = new ArrayList<String>();
	private ArrayList<String> files1 = new ArrayList<String>();
	private ArrayList<String> files2 = new ArrayList<String>();
	private ArrayList<String> files3 = new ArrayList<String>();
	private ArrayList<String> files4 = new ArrayList<String>();
	private static long startTime, interTime, endTime, totalTime;
	
	public static void main(String[] args) {
		if (args.length != 2)
			System.err.println("Usage: java WordCount [process id (0 ~ 4)] [input folder]");
		pid = Integer.parseInt(args[0]);
		fileFolder = args[1];
		
		startTime = System.currentTimeMillis();
		System.out.println("program starts with startTime in milliseconds: " + startTime);
		
		WordCount wordCount = new WordCount();
		wordCount.createZK();
		wordCount.init();
		wordCount.wordMap();
		wordCount.wordReduce();
		wordCount.output();
		wordCount.closeZK();
		
		endTime = System.currentTimeMillis();
		System.out.println("program ends with endTime in milliseconds: " + endTime);
		totalTime = (endTime - startTime) / 1000;
		System.out.println("total time is: " + (totalTime / 60) + " mins " + (totalTime % 60) + "s");
	}
		
		
	public void createZK() {	
		String host = "localhost";
		int port = 3922;
		
		Random rand = new Random();
		host = "ece-acis-dc44" + (rand.nextInt(3) + 1) + ".acis.ufl.edu";
		String addr = host + ":" + port;
		try {
			zk = new ZooKeeper(addr, this.SESSION_TIMEOUT, null);
		} catch (IOException e) {
			System.out.println("Cannot create ZooKeeper Instance");
			System.out.println("Got an exception:" + e.getMessage());
		}
	}
	
	public void closeZK() {
		try {
			zk.close();
		} catch (Exception e) {
				System.out.println("Cannot close ZooKeeper Instance");
				System.out.println("Got an exception:" + e.getMessage());
		}
	}
	
	public void init() {
		String filename, filepath;
	
		Path folder = Paths.get(fileFolder);
		try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(folder)) {
			for (Path file: dirStream) {
				filename = file.getFileName().toString();
				filepath = fileFolder + "/" + filename;
				switch (Math.abs(filename.hashCode() % 5)) {
					case 0:	files0.add(filepath); break;
					case 1: files1.add(filepath); break;
					case 2: files2.add(filepath); break;
					case 3: files3.add(filepath); break;
					case 4: files4.add(filepath); break;
					default: 
						System.out.println("something wrong happens when distributing files."); break;
				}
			}
		} catch (IOException | DirectoryIteratorException x) {
			System.err.println(x);
		}
		
		try {
			
			if (zk.exists("/words", false) == null)
				zk.create("/words", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (zk.exists("/words/440", false) == null)
				zk.create("/words/440", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (zk.exists("/words/441", false) == null)
				zk.create("/words/441", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (zk.exists("/words/442", false) == null)
				zk.create("/words/442", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (zk.exists("/words/443", false) == null)
				zk.create("/words/443", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (zk.exists("/words/444", false) == null)
				zk.create("/words/444", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (zk.exists("/output", false) == null)
				zk.create("/output", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);	
			
		} catch (InterruptedException e) {
			System.out.println("exception in init");
			System.out.println("Got an exception:" + e.getMessage());
		} catch (KeeperException e) {
			System.out.println("exception in init");
			System.out.println("Got an exception:" + e.getMessage());
		} 
		
		System.out.println("init is done.");
	}
	
	
	public void wordMap() {
		System.out.println("wordMap starts.");
		
		String word;
		ArrayList<String> files = new ArrayList<String>();
		Map<String, Integer> wordsMap = new HashMap<String, Integer>();
		
		switch (pid) {
			case 0: files = files0; break;
			case 1: files = files1; break;
			case 2: files = files2; break;
			case 3: files = files3; break;
			case 4: files = files4; break;
			default:
				System.out.println("wrong pid."); break;
		}
		
		for (String file: files) {
			try (BufferedReader buffReader = new BufferedReader(new FileReader(file))) {
				while ((word = buffReader.readLine()) != null) {
					word = word.trim();
					if (!wordsMap.containsKey(word))
						wordsMap.put(word, 1);
					else
						wordsMap.put(word, wordsMap.get(word) + 1);
						
					if (wordsMap.size() == 25000000) {
						wordShuffle(wordsMap);
						wordsMap.clear();
					}
				}
			} catch (Exception e) {
				System.out.println("error in wordMap.");
				System.out.println(e.getMessage());
			}
		}
	
		try {
			if (!wordsMap.isEmpty()) {
				wordShuffle(wordsMap);
				wordsMap.clear();
			}
			else
				zk.setData("/words/44" + pid, "shuffleDone".getBytes(), -1);
		} catch (Exception e) {
			System.out.println("error in wordShuffle.");
			System.out.println("Got an exception:" + e.getMessage());
		}
		
		System.out.println("wordMap is done.");
	}
	
	
	private void wordShuffle(Map<String, Integer> wordsMap) throws IOException, KeeperException, InterruptedException {
		int count = 0;
		Map<String, Integer> map0 = new HashMap<String, Integer>();
		Map<String, Integer> map1 = new HashMap<String, Integer>();
		Map<String, Integer> map2 = new HashMap<String, Integer>();
		Map<String, Integer> map3 = new HashMap<String, Integer>();
		Map<String, Integer> map4 = new HashMap<String, Integer>();
		
		for (Map.Entry<String, Integer> mapEntry: wordsMap.entrySet()) {
			switch (Math.abs(mapEntry.getKey().hashCode() % 5)) {
				case 0: 
					map0.put(mapEntry.getKey(), mapEntry.getValue());
					if (map0.size() == 50000) {
						zk.create("/words/440/" + pid + "-" + count, toByteArray(map0), 
								Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						count++;
						map0.clear();
					}
					break;
				case 1: 
					map1.put(mapEntry.getKey(), mapEntry.getValue());
						
					if (map1.size() == 50000) {
						zk.create("/words/441/" + pid + "-" + count, toByteArray(map1), 
								Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						count++;
						map1.clear();
					}
					break;
				case 2: 
					map2.put(mapEntry.getKey(), mapEntry.getValue());
						
					if (map2.size() == 50000) {
						zk.create("/words/442/" + pid + "-" + count, toByteArray(map2), 
								Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						count++;
						map2.clear();
					}
					break;
				case 3: 
					map3.put(mapEntry.getKey(), mapEntry.getValue());
						
					if (map3.size() == 50000) {
						zk.create("/words/443/" + pid + "-" + count, toByteArray(map3), 
								Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						count++;
						map3.clear();
					}
					break;
				case 4: 
					map4.put(mapEntry.getKey(), mapEntry.getValue());
						
					if (map4.size() == 50000) {
						zk.create("/words/444/" + pid + "-" + count, toByteArray(map4), 
								Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						count++;
						map4.clear();
					}
					break;
				default:
					System.out.println("wrong with word distributing."); break;
			}
		}
	
		try {
			if (!map0.isEmpty())
				zk.create("/words/440/" + pid + "-" + count, toByteArray(map0), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (!map1.isEmpty())
				zk.create("/words/441/" + pid + "-" + count, toByteArray(map1), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (!map2.isEmpty())
				zk.create("/words/442/" + pid + "-" + count, toByteArray(map2), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (!map3.isEmpty())
				zk.create("/words/443/" + pid + "-" + count, toByteArray(map3), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (!map4.isEmpty())
				zk.create("/words/444/" + pid + "-" + count, toByteArray(map4), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
			zk.setData("/words/44" + pid, "shuffleDone".getBytes(), -1);
			//System.out.println("set shuffledone.");

		} catch (IOException | InterruptedException | KeeperException e) {
			System.out.println("error in wordShuffle.");
			System.out.println("Got an exception:" + e.getMessage());
		}
	}
	
	
	public void wordReduce() {
		Map<String, Integer> wordsMap = new HashMap<String, Integer>();
		List<String> children;
		List<String> history = new ArrayList<String>();
		boolean isMapDone = false;
		int count = 0;
		
		System.out.println("wordReduce starts.");
		while (!isMapDone) {
			try {	
				String flag1 = new String(zk.getData("/words/440", false, null));
				String flag2 = new String(zk.getData("/words/441", false, null));
				String flag3 = new String(zk.getData("/words/442", false, null));
				String flag4 = new String(zk.getData("/words/443", false, null));
				String flag5 = new String(zk.getData("/words/444", false, null));
				
				isMapDone = flag1.contains("shuffleDone") && flag2.contains("shuffleDone") && flag3.contains("shuffleDone") 
						&& flag4.contains("shuffleDone") && flag5.contains("shuffleDone");
				//System.out.println(isMapDone);			
				children = zk.getChildren("/words/44" + pid, false);
				for (String str: children) {
					if (history.contains(str))
						continue;
					history.add(str);
					Map<String, Integer> map = toMap(zk.getData("/words/44" + pid + "/" + str, false, null));
					for (Map.Entry<String, Integer> mapEntry: map.entrySet()) {
						if (!wordsMap.containsKey(mapEntry.getKey()))
							wordsMap.put(mapEntry.getKey(), mapEntry.getValue());
						else
							wordsMap.put(mapEntry.getKey(), mapEntry.getValue() + wordsMap.get(mapEntry.getKey()));
							
						if (wordsMap.size() == 50000000) {
							mapFlush("mapOut" + count + ".txt", wordsMap);
							count++;
							wordsMap.clear();
						}	
					}
				}
			} catch (IOException | KeeperException | InterruptedException e) {
				System.out.println("error in wordReduce");
				System.out.println("Got an exception:" + e.getMessage());
			} catch (ClassNotFoundException e) {
				System.out.println("error in wordReduce for toMap()");
				System.out.println("Got an exception:" + e.getMessage());
			}
		}
		
		int i, index, value;
		String entry, word;
		if (count != 0) {
			for (i = 0; i < count; i++) {
				try (BufferedReader buffReader = new BufferedReader(new FileReader("mapOut" + i + ".txt"))) {
					while ((entry = buffReader.readLine()) != null) {
						index = entry.indexOf("\t");
						word = entry.substring(0, index);
						value = Integer.valueOf(entry.substring(index + 1));
						if (!wordsMap.containsKey(word))
							wordsMap.put(word, value);
						else
							wordsMap.put(word, value + wordsMap.get(word));
					}
				} catch (Exception e) {
					System.out.println("error in wordReduce.");
					System.out.println("Got an exception:" + e.getMessage());
				}
			}
		}
		
		//mapFlush("output" + pid + ".txt", wordsMap);
		Map<String, Integer> map = new HashMap<String, Integer>();
		int num = 0;
		
		try {
			for (Map.Entry<String, Integer> mapEntry: wordsMap.entrySet()) {
				map.put(mapEntry.getKey(), mapEntry.getValue());
				if (map.size() == 50000) {
					zk.create("/output/" + pid + "-" + num, toByteArray(map), 
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					num++;
					map.clear();
				}
			}
		
			if (!map.isEmpty())
				zk.create("/output/" + pid + "-" + num, toByteArray(map), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			
			zk.setData("/words/44" + pid, "shuffleDone reduceDone".getBytes(), -1);
		} catch (IOException | InterruptedException | KeeperException e) {
			System.out.println("error in wordReduce.");
			System.out.println("Got an exception:" + e.getMessage());
		}
		
		/*
		interTime = (System.currentTimeMillis() - startTime) / 1000;
		System.out.println("word process is done. The time is: " + (interTime / 60) + " mins " + (interTime % 60) + "s");
		*/
	}
	
	public void output() {
		List<String> children;
		List<String> history = new ArrayList<String>();
		boolean isDone = false;
		
		while (!isDone) {
			try {	
				String flag1 = new String(zk.getData("/words/440", false, null));
				String flag2 = new String(zk.getData("/words/441", false, null));
				String flag3 = new String(zk.getData("/words/442", false, null));
				String flag4 = new String(zk.getData("/words/443", false, null));
				String flag5 = new String(zk.getData("/words/444", false, null));
	
				isDone = flag1.contains("reduceDone") && flag2.contains("reduceDone") && flag3.contains("reduceDone") 
						&& flag4.contains("reduceDone") && flag5.contains("reduceDone");
						
				children = zk.getChildren("/output", false);
				for (String str: children) {
					if (history.contains(str))
						continue;
					history.add(str);
					Map<String, Integer> map = toMap(zk.getData("/output" + "/" + str, false, null));
					mapFlush("result" + pid + ".txt", map);
				}
			} catch (IOException | KeeperException | InterruptedException e) {
				System.out.println("error in output");
				System.out.println("Got an exception:" + e.getMessage());
			} catch (ClassNotFoundException e) {
				System.out.println("error in output for toMap()");
				System.out.println("Got an exception:" + e.getMessage());
			}
		}
	}
	
	
	private byte[] toByteArray(Map<String, Integer> map) throws IOException {
		byte[] bytes = null;
		try (	ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);) {
			oos.writeObject(map);
			oos.flush();
			bytes = bos.toByteArray();
		}
		return bytes;
	}
	
	
	private Map<String, Integer> toMap(byte[] bytes) throws IOException, ClassNotFoundException {
		Map<String, Integer> map;
		try (	ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
			ObjectInputStream ois = new ObjectInputStream(bis);) {
			//@SuppressWarnings("unchecked")
			map = (Map<String, Integer>) ois.readObject();
		}
		return map;
	}
	
	
	private void mapFlush(String outputFile, Map<String, Integer> wordsMap) {
		try (PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(outputFile, true)))) {
			for (Map.Entry<String, Integer> mapEntry: wordsMap.entrySet())
				printWriter.println(mapEntry.getKey() + "\t" + mapEntry.getValue());
		} catch (Exception e) {
			System.out.println("error in mapFlush.");
		}
	}
	
}
