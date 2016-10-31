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
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.ctrl.TopicManager;
import org.cripac.isee.vpe.debug.FakePedestrianAttrRecognizer;
import org.cripac.isee.vpe.debug.FakePedestrianTracker;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
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
 *
 * Created by ken.yu on 16-10-31.
 */
public class DataManagingAppTest {

    private KafkaProducer<String, byte[]> producer;
    private ConsoleLogger logger;

    @Before
    public void init() throws Exception {
        init(new String[0]);
    }

    public void init(String[] args) throws Exception {
        SystemPropertyCenter propCenter = new SystemPropertyCenter(args);

        TopicManager.checkTopics(propCenter);

        Properties producerProp = new Properties();
        producerProp.put("bootstrap.servers", propCenter.kafkaBrokers);
        producerProp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProp.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(producerProp);
        logger = new ConsoleLogger();
    }

//    @Test
    public void testTrackletSaving() throws Exception {
        TaskData.ExecutionPlan plan = new TaskData.ExecutionPlan();
        TaskData.ExecutionPlan.Node savingNode =
                plan.addNode(DataManagingApp.PedestrainTrackletRetrievingStream.INFO);
        TaskData data = new TaskData(savingNode, plan,
                new FakePedestrianTracker().track(null));
        sendWithLog(DataManagingApp.PedestrainTrackletRetrievingStream.PED_TRACKLET_RTRV_JOB_TOPIC,
                UUID.randomUUID().toString(),
                serialize(data),
                producer,
                logger);
    }

    //    @Test
    public void testAttributeSaving() throws Exception {
        TaskData.ExecutionPlan plan = new TaskData.ExecutionPlan();
        TaskData.ExecutionPlan.Node savingNode =
                plan.addNode(DataManagingApp.PedestrainTrackletRetrievingStream.INFO);
        TaskData data = new TaskData(savingNode, plan,
                new FakePedestrianAttrRecognizer().recognize(new FakePedestrianTracker().track(null)[0]));
        sendWithLog(DataManagingApp.PedestrainTrackletRetrievingStream.PED_TRACKLET_RTRV_JOB_TOPIC,
                UUID.randomUUID().toString(),
                serialize(data),
                producer,
                logger);
    }

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
            test.testAttributeSaving();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
