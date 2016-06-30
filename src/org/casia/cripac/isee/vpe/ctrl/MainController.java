/***********************************************************************
 * This file is part of VPE-Platform.
 * 
 * VPE-Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * VPE-Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with VPE-Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/

package org.casia.cripac.isee.vpe.ctrl;
/**
 * 
 */

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;

import org.I0Itec.zkclient.ZkClient;
import org.casia.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;

import kafka.admin.AdminUtils;

/**
 * The MainController class initializes the system environment,
 * then starts the message handling applications and command simulating program respectively.
 * TODO After development of the whole system, modify this to start all the applications.
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
