package org.isee.vpe.ctrl;
/**
 * 
 */

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;

import kafka.admin.AdminUtils;

/**
 * @author ken
 * 
 */
public class MainController implements Serializable {
	private static final long serialVersionUID = 4145646848594916984L;
	
	//Zookeeper properties
	String zookeeper = "localhost:2181";
	int sessionTimeoutMs = 10 * 10000;
	int connectionTimeoutMs = 8 * 1000;
	
	String brokers = "localhost:9092";
	int partitions = 1;
	int replicationFactor = 1;
	HashSet<String> topics = new HashSet<>();
	
	private void loadSystemProperties(String propertyFilename) throws IOException {
		Properties systemProperties = new Properties();
		BufferedInputStream propInputStream = new BufferedInputStream(new FileInputStream(propertyFilename));
		systemProperties.load(propInputStream);
		
		for (Entry<Object, Object> entry : systemProperties.entrySet()) {
			switch ((String) entry.getKey()) {
			case "zookeeper":
				zookeeper = (String) entry.getValue(); 
				break;
			case "brokers":
				brokers = (String) entry.getValue(); 
				break;
			case "partitions":
				partitions = new Integer((String) entry.getValue()); 
				break;
			case "replicationFactor":
				replicationFactor = new Integer((String) entry.getValue()); 
				break;
			}
		}
	}
	
	MainController() throws IOException {
		loadSystemProperties("system.properties");
		

		ZkClient zkClient = new ZkClient(zookeeper, sessionTimeoutMs, connectionTimeoutMs);
		
		//Create topics.
		for (String topic : topics) {
			if (!AdminUtils.topicExists(zkClient, topic)) {
				AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, new Properties());
			}
		}
	}
	
	void run() {
		CommandHandlingApplication commandHandlingApplication = new CommandHandlingApplication(brokers);
		commandHandlingApplication.start();

		CommandGenerator commandGenerator = new CommandGenerator(brokers);
		commandGenerator.generatePresetCommand();
		
		commandHandlingApplication.stop();
	}
	
	/**
	 * @param args No options supported currently.
	 */
	public static void main(String[] args) {
		MainController controller;
		try {
			controller = new MainController();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		controller.run();
	}

}
