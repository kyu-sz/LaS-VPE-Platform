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

import java.util.HashSet;

import org.I0Itec.zkclient.ZkClient;
import org.casia.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.casia.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;

import kafka.admin.AdminUtils;

/**
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class TopicManager {

	public static void checkTopics(SystemPropertyCenter propertyCenter) {
		//Initializes the system environment.
		System.out.println("Connecting to zookeeper: " + propertyCenter.zookeeper);
		ZkClient zkClient = new ZkClient(
				propertyCenter.zookeeper,
				propertyCenter.sessionTimeoutMs,
				propertyCenter.connectionTimeoutMs);

		//Create topics.
		HashSet<String> topics = new HashSet<>();
		topics.add(MessageHandlingApp.COMMAND_TOPIC);
		topics.add(PedestrianTrackingApp.PEDESTRIAN_TRACKING_TASK_TOPIC);
		topics.add(PedestrianAttrRecogApp.PEDESTRIAN_ATTR_RECOG_INPUT_TOPIC);
		topics.add(PedestrianAttrRecogApp.PEDESTRIAN_ATTR_RECOG_TASK_TOPIC);
		topics.add(MetadataSavingApp.PEDESTRIAN_ATTR_SAVING_INPUT_TOPIC);
		topics.add(MetadataSavingApp.PEDESTRIAN_TRACK_SAVING_INPUT_TOPIC);
		for (String topic : topics) {
			if (!AdminUtils.topicExists(zkClient, topic)) {
				System.out.println("Creating topic: " + topic);
				kafka.admin.TopicCommand.main(new String[]{
						"--create",
						"--zookeeper", propertyCenter.zookeeper,
						"--topic", topic,
						"--partitions", new Integer(propertyCenter.kafkaPartitions).toString(),
						"--replication-factor", new Integer(propertyCenter.kafkaReplicationFactor).toString()
						});
			}
		}
	}
}
