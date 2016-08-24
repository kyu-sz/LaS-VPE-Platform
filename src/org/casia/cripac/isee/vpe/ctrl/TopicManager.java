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
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;

import kafka.admin.AdminUtils;

/**
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class TopicManager {

	private static HashSet<String> topics = new HashSet<>();

	public static void registerTopic(String topic) {
		topics.add(topic);
	}

	public static void checkTopics(SystemPropertyCenter propertyCenter) {

		// Check each node of the zookeeper cluster respectively.
		String[] znodes = propertyCenter.zookeeperConnect.split(",");
		for (String znode : znodes) {
			// Initialize the system environment.
			System.out.println("|INFO|Connecting to zookeeper: " + znode);
			ZkClient zkClient = new ZkClient(znode, propertyCenter.sessionTimeoutMs,
					propertyCenter.connectionTimeoutMs);
			for (String topic : topics) {
				System.out.println("|INFO|Checking topic: " + topic);
				if (!AdminUtils.topicExists(zkClient, topic)) {
					System.out.println("Creating topic: " + topic);
					kafka.admin.TopicCommand.main(new String[] { "--create", "--zookeeper", znode, "--topic", topic,
							"--partitions", "" + propertyCenter.kafkaPartitions, "--replication-factor",
							"" + propertyCenter.kafkaReplicationFactor });
				}
			}
		}
		System.out.println("|INFO|Topics checked!");
	}
}
