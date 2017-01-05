/***********************************************************************
 * This file is part of LaS-VPE Platform.
 *
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/

package org.cripac.isee.vpe.ctrl;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.cripac.isee.vpe.common.Topic;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Ken Yu, CRIPAC, 2016
 */
public class TopicManager {

    private static Set<Topic> topics = new HashSet<>();

    /**
     * Private constructor - this class will never be instanced
     */
    private TopicManager() {
    }

    public static class DuplicateTopicNameError extends Error {
        public DuplicateTopicNameError(@Nonnull String s) {
            super(s);
        }
    }

    public static void registerTopic(@Nonnull Topic topic) {
        if (topics.contains(topic)) {
            throw new DuplicateTopicNameError("Duplicate declaration of topic " + topic.NAME);
        }
        topics.add(topic);
    }

    public static void checkTopics(@Nonnull SystemPropertyCenter propCenter) {
        System.out.println("[INFO]Connecting to zookeeper: " + propCenter.zkConn);
        ZkConnection zkConn = new ZkConnection(propCenter.zkConn, propCenter.sessionTimeoutMs);
        ZkClient zkClient = new ZkClient(zkConn);
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConn, false);
        for (Topic topic : topics) {
            System.out.println("[INFO]Checking topic: " + topic);
            if (!AdminUtils.topicExists(zkUtils, topic.NAME)) {
                // AdminUtils.createTopic(zkClient, topic,
                // propCenter.kafkaNumPartitions,
                // propCenter.kafkaReplFactor, new Properties());
                System.out.println("[INFO]Creating topic: " + topic);
                kafka.admin.TopicCommand.main(
                        new String[]{
                                "--create",
                                "--zookeeper", propCenter.zkConn,
                                "--topic", topic.NAME,
                                "--partitions", "" + propCenter.kafkaNumPartitions,
                                "--replication-factor", "" + propCenter.kafkaReplFactor});
            }
        }
        System.out.println("[INFO]Topics checked!");
    }
}
