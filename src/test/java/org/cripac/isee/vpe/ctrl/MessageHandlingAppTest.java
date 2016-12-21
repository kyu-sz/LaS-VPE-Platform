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

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.cripac.isee.vpe.common.LoginParam;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.junit.Before;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Hashtable;
import java.util.Properties;

import static org.apache.commons.lang3.SerializationUtils.serialize;
import static org.cripac.isee.vpe.util.kafka.KafkaHelper.sendWithLog;

/**
 * The MessageHandlingAppTest class is for simulating commands sent to the message
 * handling application through Kafka.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class MessageHandlingAppTest implements Serializable {

    private KafkaProducer<String, byte[]> producer;
    private ConsoleLogger logger;

    public static void main(String[] args) throws Exception {
        MessageHandlingAppTest app = new MessageHandlingAppTest();
        app.init(args);
        app.generatePresetCommand();
    }

    @Before
    public void init() throws Exception {
        init(new String[0]);
    }

    public void init(String[] args) throws Exception {
        SystemPropertyCenter propCenter;
        if (args.length > 0) {
            propCenter = new SystemPropertyCenter(args);
        } else {
            propCenter = new SystemPropertyCenter();
        }

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
    public void generatePresetCommand() throws Exception {
        Hashtable<String, Serializable> param = new Hashtable<>();
        param.put(MessageHandlingApp.Parameter.TRACKING_CONF_FILE,
                "pedestrian-tracking-isee-basic-CAM01_0.conf");
        param.put(MessageHandlingApp.Parameter.VIDEO_URL,
                "source_data/video/CAM01/2014_04_25/20140425184816-20140425190532.h264");
        param.put(MessageHandlingApp.Parameter.TRACKLET_SERIAL_NUM, "1");
        param.put(MessageHandlingApp.Parameter.WEBCAM_LOGIN_PARAM,
                new Gson().toJson(new LoginParam(InetAddress.getLocalHost(), 0,
                        "Ken Yu", "I love Shenzhen!")));

        sendWithLog(MessageHandlingApp.MessageHandlingStream.COMMAND_TOPIC,
                MessageHandlingApp.CommandType.TRACK_ONLY,
                serialize(param),
                producer,
                logger);

        sendWithLog(MessageHandlingApp.MessageHandlingStream.COMMAND_TOPIC,
                MessageHandlingApp.CommandType.ATTRRECOG_ONLY,
                serialize(param),
                producer,
                logger);

        sendWithLog(MessageHandlingApp.MessageHandlingStream.COMMAND_TOPIC,
                MessageHandlingApp.CommandType.TRACK_ATTRRECOG,
                serialize(param),
                producer,
                logger);

        sendWithLog(MessageHandlingApp.MessageHandlingStream.COMMAND_TOPIC,
                MessageHandlingApp.CommandType.REID_ONLY,
                serialize(param),
                producer,
                logger);

        sendWithLog(MessageHandlingApp.MessageHandlingStream.COMMAND_TOPIC,
                MessageHandlingApp.CommandType.ATTRRECOG_REID,
                serialize(param),
                producer,
                logger);

        sendWithLog(MessageHandlingApp.MessageHandlingStream.COMMAND_TOPIC,
                MessageHandlingApp.CommandType.TRACK_ATTRRECOG_REID,
                serialize(param),
                producer,
                logger);
    }
}
