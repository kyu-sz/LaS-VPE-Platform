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

package org.cripac.isee.vpe.data;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.cripac.isee.pedestrian.attr.Attributes;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.ctrl.TopicManager;
import org.cripac.isee.vpe.debug.FakePedestrianAttrRecognizer;
import org.cripac.isee.vpe.debug.FakePedestrianTracker;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.junit.Before;

import java.util.Properties;
import java.util.UUID;

import static org.cripac.isee.vpe.util.SerializationHelper.serialize;
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
public class DataManagingAppTest {

    private KafkaProducer<String, byte[]> producer;
    private ConsoleLogger logger;

    public static void main(String[] args) {
        DataManagingAppTest test = new DataManagingAppTest();
        try {
            test.init(args);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        try {
            test.testTrackletSaving();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            test.testAttrSaving();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Before
    public void init() throws Exception {
        init(new String[0]);
    }

    public void init(String[] args) throws Exception {
        SystemPropertyCenter propCenter = new SystemPropertyCenter(args);

        TopicManager.checkTopics(propCenter);

        Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                propCenter.kafkaBootstrapServers);
        producerProp.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                propCenter.kafkaMaxRequestSize);
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
        producerProp.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                "" + propCenter.kafkaMsgMaxBytes);
        producer = new KafkaProducer<>(producerProp);
        logger = new ConsoleLogger(Level.DEBUG);
    }

    //    @Test
    public void testTrackletSaving() throws Exception {
        TaskData.ExecutionPlan plan = new TaskData.ExecutionPlan();
        TaskData.ExecutionPlan.Node savingNode = plan.addNode(DataManagingApp.SavingStream.INFO);

        Tracklet[] tracklets = new FakePedestrianTracker().track(new byte[0]);
        String taskID = UUID.randomUUID().toString();
        for (int i = 0; i < tracklets.length; ++i) {
            Tracklet tracklet = tracklets[i];
            tracklet.id = new Tracklet.Identifier("fake", i);

            TaskData data = new TaskData(savingNode, plan, tracklet);
            sendWithLog(DataManagingApp.SavingStream.PED_TRACKLET_SAVING_TOPIC,
                    taskID,
                    serialize(data),
                    producer,
                    logger);
        }
    }

    //    @Test
    public void testAttrSaving() throws Exception {
        TaskData.ExecutionPlan plan = new TaskData.ExecutionPlan();
        TaskData.ExecutionPlan.Node savingNode = plan.addNode(DataManagingApp.SavingStream.INFO);

        Attributes attributes = new FakePedestrianAttrRecognizer().recognize(
                new FakePedestrianTracker().track(new byte[0])[0]);
        attributes.trackletID = new Tracklet.Identifier("fake", 0);
        assert attributes != null;

        TaskData data = new TaskData(savingNode, plan, attributes);
        sendWithLog(DataManagingApp.SavingStream.PED_ATTR_SAVING_TOPIC,
                UUID.randomUUID().toString(),
                serialize(data),
                producer,
                logger);
    }
}
