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
import org.isee.vpe.alg.PedestrianTrackingApp;

import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;

/**
 * @author Kai Yu
 * 
 */
public class MainController implements Serializable {
	private static final long serialVersionUID = 4145646848594916984L;
	
	//Zookeeper properties
	String zookeeper = "localhost:2181";
	int sessionTimeoutMs = 10 * 10000;
	int connectionTimeoutMs = 8 * 1000;
	
	//Kafka properties
	String kafkaBrokers = "localhost:9092";
	int partitions = 1;
	int replicationFactor = 1;
	HashSet<String> topics = new HashSet<>();
	
	//Spark properties
	String checkpointDir = "checkpoint";
	String sparkMaster = "local[*]";
	
	private void loadSystemProperties(String propertyFilename) throws IOException {
		Properties systemProperties = new Properties();
		BufferedInputStream propInputStream = new BufferedInputStream(new FileInputStream(propertyFilename));
		systemProperties.load(propInputStream);
		
		for (Entry<Object, Object> entry : systemProperties.entrySet()) {
			
			System.out.println("Read property: " + entry.getKey() + " - " + entry.getValue());
			
			switch ((String) entry.getKey()) {
			case "zookeeper":
				zookeeper = (String) entry.getValue(); 
				break;
			case "kafka.brokers":
				kafkaBrokers = (String) entry.getValue(); 
				break;
			case "partitions":
				partitions = new Integer((String) entry.getValue()); 
				break;
			case "replication.factor":
				replicationFactor = new Integer((String) entry.getValue()); 
				break;
			case "checkpoint.directory":
				checkpointDir = (String) entry.getValue(); 
				break;
			case "spark.master":
				sparkMaster = (String) entry.getValue(); 
				break;
			}
		}
	}
	
	MainController() throws IOException {
		loadSystemProperties("system.properties");
		
		System.out.println("Connecting to zookeeper: " + zookeeper);
		ZkClient zkClient = new ZkClient(zookeeper, sessionTimeoutMs, connectionTimeoutMs);
		
		topics.add(MessageHandlingApp.COMMAND_TOPIC);
		topics.add(PedestrianTrackingApp.TRACKING_TASK_TOPIC);
		//Create topics.
		for (String topic : topics) {
			if (!AdminUtils.topicExists(zkClient, topic)) {
				System.out.println("Creating topic: " + topic);
//				AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, new Properties());
				kafka.admin.TopicCommand.main(new String[]{
						"--create",
						"--zookeeper", zookeeper,
						"--topic", topic,
						"--partitions", new Integer(partitions).toString(),
						"--replication-factor", new Integer(replicationFactor).toString()
						});
			}
		}
	}
	
	void run() {
		MessageHandlingApp messageHandlingApplication = new MessageHandlingApp(sparkMaster, kafkaBrokers);
		messageHandlingApplication.initialize(checkpointDir);
		messageHandlingApplication.start();

		CommandGenerator commandGenerator = new CommandGenerator(kafkaBrokers);
		commandGenerator.generatePresetCommand();
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		messageHandlingApplication.stop();
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
