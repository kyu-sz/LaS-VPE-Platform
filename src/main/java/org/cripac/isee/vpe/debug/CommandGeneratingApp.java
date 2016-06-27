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

package org.cripac.isee.vpe.debug;

import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.cripac.isee.vpe.ctrl.MessageHandlingApp;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TopicManager;
import org.cripac.isee.vpe.util.SerializationHelper;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import static org.cripac.isee.vpe.util.kafka.KafkaHelper.sendWithLog;

/**
 * The CommandGenerator class is for simulating commands sent to the message
 * handling application through Kafka.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class CommandGeneratingApp implements Serializable {

    public static final String APP_NAME = "CommandGenerating";
    private static final long serialVersionUID = -1221111574183021547L;
    private Singleton<KafkaProducer<String, byte[]>> producerSingleton;
    private ConsoleLogger logger;

    public CommandGeneratingApp(SystemPropertyCenter propCenter) throws Exception {
        Properties producerProp = new Properties();
        producerProp.put("bootstrap.servers", propCenter.kafkaBrokers);
        producerProp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProp.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerSingleton = new Singleton<>(new KafkaProducerFactory<String, byte[]>(producerProp));
        logger = new ConsoleLogger();
    }

    public static void main(String[] args) throws Exception {
        SystemPropertyCenter propCenter;
        if (args.length > 0) {
            propCenter = new SystemPropertyCenter(args);
        } else {
            propCenter = new SystemPropertyCenter();
        }

        TopicManager.checkTopics(propCenter);

        CommandGeneratingApp app = new CommandGeneratingApp(propCenter);
        app.generatePresetCommand();
    }

    public void generatePresetCommand() throws Exception {
        Map<String, String> param = new HashedMap();
        param.put(
                MessageHandlingApp.Parameter.TRACKING_CONF_FILE,
                "pedestrian-tracking-isee-basic-CAM01_0.conf");
        param.put(
                MessageHandlingApp.Parameter.VIDEO_URL,
                "source_data/video/CAM01/2014_04_25/20140425184816-20140425190532.h264");
        param.put(MessageHandlingApp.Parameter.TRACKLET_SERIAL_NUM, "1");

        sendWithLog(MessageHandlingApp.MessageHandlingStream.COMMAND_TOPIC,
                MessageHandlingApp.CommandType.TRACK_ONLY,
                SerializationHelper.serialize(param),
                producerSingleton.getInst(),
                logger);

        sendWithLog(MessageHandlingApp.MessageHandlingStream.COMMAND_TOPIC,
                MessageHandlingApp.CommandType.RECOG_ATTR_ONLY,
                SerializationHelper.serialize(param),
                producerSingleton.getInst(),
                logger);

        sendWithLog(MessageHandlingApp.MessageHandlingStream.COMMAND_TOPIC,
                MessageHandlingApp.CommandType.TRACK_AND_RECOG_ATTR,
                SerializationHelper.serialize(param),
                producerSingleton.getInst(),
                logger);

        sendWithLog(MessageHandlingApp.MessageHandlingStream.COMMAND_TOPIC,
                MessageHandlingApp.CommandType.REID_ONLY,
                SerializationHelper.serialize(param),
                producerSingleton.getInst(),
                logger);

        sendWithLog(MessageHandlingApp.MessageHandlingStream.COMMAND_TOPIC,
                MessageHandlingApp.CommandType.RECOG_ATTR_AND_REID,
                SerializationHelper.serialize(param),
                producerSingleton.getInst(),
                logger);

        sendWithLog(MessageHandlingApp.MessageHandlingStream.COMMAND_TOPIC,
                MessageHandlingApp.CommandType.TRACK_AND_RECOG_ATTR_AND_REID,
                SerializationHelper.serialize(param),
                producerSingleton.getInst(),
                logger);
    }
}
