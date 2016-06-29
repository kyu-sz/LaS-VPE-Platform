package org.isee.vpe.ctrl;
/**
 * 
 */

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;

import org.I0Itec.zkclient.ZkClient;
import org.isee.vpe.alg.PedestrianTrackingApp;
import org.isee.vpe.common.SystemPropertyCenter;

import kafka.admin.AdminUtils;

/**
 * @author Ken Yu
 * 
 */
public class MainController implements Serializable {
	private static final long serialVersionUID = 4145646848594916984L;
	private SystemPropertyCenter propertyCenter = null;
	
	public HashSet<String> topics = new HashSet<>();
	
	MainController() throws IOException {
		//Load system properties.
		propertyCenter = new SystemPropertyCenter("system.properties");

		//Initializes the system environment.
		System.out.println("Connecting to zookeeper: " + propertyCenter.zookeeper);
		ZkClient zkClient = new ZkClient(
				propertyCenter.zookeeper,
				propertyCenter.sessionTimeoutMs,
				propertyCenter.connectionTimeoutMs);

		//Create topics.
		topics.add(MessageHandlingApp.COMMAND_TOPIC);
		topics.add(PedestrianTrackingApp.TRACKING_TASK_TOPIC);
		topics.add(PedestrianTrackingApp.TRACKING_RESULT_TOPIC);
		for (String topic : topics) {
			if (!AdminUtils.topicExists(zkClient, topic)) {
				System.out.println("Creating topic: " + topic);
				kafka.admin.TopicCommand.main(new String[]{
						"--create",
						"--zookeeper", propertyCenter.zookeeper,
						"--topic", topic,
						"--partitions", new Integer(propertyCenter.partitions).toString(),
						"--replication-factor", new Integer(propertyCenter.replicationFactor).toString()
						});
			}
		}
	}
	
	void run() {
		//Create and start a message handling application.
		MessageHandlingApp messageHandlingApplication =
				new MessageHandlingApp(propertyCenter.sparkMaster, propertyCenter.kafkaBrokers);
		messageHandlingApplication.initialize(propertyCenter.checkpointDir);
		messageHandlingApplication.start();

		//Create and start a command generator
		//to simulate commands sent to the message handling application through Kafka.
		CommandGenerator commandGenerator = new CommandGenerator(propertyCenter.kafkaBrokers);
		commandGenerator.generatePresetCommand();
		
		//Wait some time for the whole system to digest the commands.
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		//Stop the system.
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
