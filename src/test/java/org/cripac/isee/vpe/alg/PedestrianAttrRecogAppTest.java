/*
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
 */

package org.cripac.isee.vpe.alg;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.log4j.Level;
import org.cripac.isee.pedestrian.attr.DeepMARTest;
import org.cripac.isee.pedestrian.attr.ExternPedestrianAttrRecognizerTest;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.debug.FakePedestrianTracker;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.junit.Before;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.cripac.isee.vpe.util.SerializationHelper.deserialize;
import static org.cripac.isee.vpe.util.kafka.KafkaHelper.sendWithLog;

/**
 * This is a JUnit test for the DataManagingApp.
 * Different from usual JUnit tests, this test does not initiate a DataManagingApp.
 * The application should be run on YARN in advance.
 * This test only sends fake data messages to and receives results
 * from the already running application through Kafka.
 * <p>
 * Created by ken.yu on 16-10-31.
 */
public class PedestrianAttrRecogAppTest {

    private static final Stream.Port TEST_PED_ATTR_RECV_PORT =
            new Stream.Port("test-pedestrian-attr-recv", DataType.ATTRIBUTES);

    private KafkaProducer<String, byte[]> producer;
    private KafkaConsumer<String, byte[]> consumer;
    private ConsoleLogger logger;
    private InetAddress externAttrRecogServerAddr;
    private int externAttrRecogServerPort = 0;
    private PedestrianAttrRecogApp.AppPropertyCenter propCenter;
    private static boolean toTestApp = true;

    public static void main(String[] args) {
        PedestrianAttrRecogAppTest test = new PedestrianAttrRecogAppTest();
        try {
            test.init(args);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        try {
            test.testDeepMAR();
            test.testExternAttrReognizer();
            if (toTestApp) {
                test.testAttrRecogApp();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void checkTopic(String topic) {
        Logger logger = new ConsoleLogger(Level.DEBUG);
        logger.info("Connecting to zookeeper: " + propCenter.zkConn);
        ZkConnection zkConn = new ZkConnection(propCenter.zkConn, propCenter.zkSessionTimeoutMs);
        ZkClient zkClient = new ZkClient(zkConn, propCenter.zkConnectionTimeoutMS);
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConn, JaasUtils.isZkSecurityEnabled());
        logger.info("Checking topic: " + topic);
        KafkaHelper.createTopicIfNotExists(zkUtils,
                topic,
                propCenter.kafkaNumPartitions,
                propCenter.kafkaReplFactor);
    }

    @Before
    public void init() throws Exception {
        init(new String[0]);
    }

    private void init(String[] args) throws ParserConfigurationException, UnknownHostException, SAXException, URISyntaxException {
        logger = new ConsoleLogger(Level.DEBUG);

        propCenter = new PedestrianAttrRecogApp.AppPropertyCenter(args);
        externAttrRecogServerAddr = propCenter.externAttrRecogServerAddr;
        externAttrRecogServerPort = propCenter.externAttrRecogServerPort;

        checkTopic(TEST_PED_ATTR_RECV_PORT.inputType.name());

        try {
            Properties producerProp = propCenter.getKafkaProducerProp(false);
            producer = new KafkaProducer<>(producerProp);

            Properties consumerProp = propCenter.getKafkaConsumerProp(UUID.randomUUID().toString(), false);
            consumer = new KafkaConsumer<>(consumerProp);
            consumer.subscribe(Collections.singletonList(TEST_PED_ATTR_RECV_PORT.inputType.name()));
        } catch (Exception e) {
            logger.error("When checking topics", e);
            logger.info("App test is disabled.");
            toTestApp = false;
        }
    }

    //    @Test
    public void testExternAttrReognizer() throws Exception {
        if (propCenter.algorithm == PedestrianAttrRecogApp.Algorithm.EXT) {
            logger.info("Using external pedestrian attribute recognizer.");

            ExternPedestrianAttrRecognizerTest test = new ExternPedestrianAttrRecognizerTest();
            test.setUp();
            test.recognize();
        }
    }

    public void testDeepMAR() throws Exception {
        if (propCenter.algorithm == PedestrianAttrRecogApp.Algorithm.DeepMAR) {
            logger.info("Using DeepMAR for pedestrian attribute recognition.");

            DeepMARTest test = new DeepMARTest();
            test.setUp();
            test.recognize();
        }
    }

    //    @Test
    public void testAttrRecogApp() throws Exception {
        logger.info("Testing attr recogn app.");
        TaskData.ExecutionPlan plan = new TaskData.ExecutionPlan();
        TaskData.ExecutionPlan.Node recogNode = plan.addNode(PedestrianAttrRecogApp.RecogStream.OUTPUT_TYPE);
        TaskData.ExecutionPlan.Node attrSavingNode = plan.addNode(DataType.NONE);
        recogNode.outputTo(attrSavingNode.createInputPort(TEST_PED_ATTR_RECV_PORT));

        // Send request (fake tracklet).
        TaskData trackletData = new TaskData(recogNode.getOutputPorts(), plan,
                new FakePedestrianTracker().track(null)[0]);
        assert trackletData.predecessorRes != null && trackletData.predecessorRes instanceof Tracklet;
        sendWithLog(UUID.randomUUID().toString(),
                trackletData,
                producer,
                logger);

        logger.info("Waiting for response...");
        // Receive result (attributes).
        ConsumerRecords<String, byte[]> records;
        while (true) {
            records = consumer.poll(0);
            if (records.isEmpty()) {
                continue;
            }

            logger.info("Response received!");
            records.forEach(rec -> {
                TaskData taskData;
                try {
                    taskData = deserialize(rec.value());
                } catch (Exception e) {
                    logger.error("During TaskData deserialization", e);
                    return;
                }
                if (taskData.destPorts.containsKey(TEST_PED_ATTR_RECV_PORT)) {
                    logger.info("<" + rec.topic() + ">\t" + rec.key() + "\t-\t" + taskData.predecessorRes);
                }
            });

            consumer.commitSync();
        }
    }
}
