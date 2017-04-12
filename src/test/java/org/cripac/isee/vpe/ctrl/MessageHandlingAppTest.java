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

package org.cripac.isee.vpe.ctrl;

import com.google.gson.Gson;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Level;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.LoginParam;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.junit.Before;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    private static final long serialVersionUID = 6788686506662339278L;
    private KafkaProducer<String, byte[]> producer;
    private ConsoleLogger logger;

    public static void main(String[] args) throws Exception {
        MessageHandlingAppTest app = new MessageHandlingAppTest();
        app.init(args);
        app.generatePresetCommand();
    }

    @Before
    public void init() throws Exception {
        init(new String[]{"-a", MessageHandlingApp.APP_NAME,
                "--system-property-file", "conf/system.properties",
                "--app-property-file", "conf/" + MessageHandlingApp.APP_NAME + "/app.properties"});
    }

    private void init(String[] args) throws Exception {
        List<String> argList = new ArrayList<>(Arrays.asList(args));
        argList.add("-a");
        argList.add(MessageHandlingApp.APP_NAME);
        args = new String[argList.size()];
        argList.toArray(args);
        SystemPropertyCenter propCenter = new SystemPropertyCenter(args);

        Properties producerProp = propCenter.getKafkaProducerProp(false);
        producer = new KafkaProducer<>(producerProp);
        logger = new ConsoleLogger(Level.DEBUG);
    }

    //    @Test
    public void generatePresetCommand() throws Exception {
        String[] cam01VideoURLs = {
                "source_data/video/CAM01/2013-12-23/20131223102739-20131223103331.h264",
                "source_data/video/CAM01/2013-12-23/20131223103331-20131223103919.h264",
                "source_data/video/CAM01/2013-12-23/20131223103919-20131223104515.h264",
                "source_data/video/CAM01/2013-12-23/20131223104515-20131223105107.h264",
                "source_data/video/CAM01/2013-12-23/20131223105107-20131223105659.h264",
                "source_data/video/CAM01/2013-12-23/20131223105659-20131223110255.h264",
                "source_data/video/CAM01/2013-12-23/20131223110255-20131223110847.h264",
                "source_data/video/CAM01/2013-12-23/20131223110847-20131223111447.h264",
                "source_data/video/CAM01/2013-12-23/20131223111447-20131223112043.h264",
                "source_data/video/CAM01/2013-12-23/20131223112043-20131223112635.h264",
        };

        Object2ObjectOpenHashMap<String, Serializable> param = new Object2ObjectOpenHashMap<>();
        param.put(MessageHandlingApp.Parameter.TRACKING_CONF_FILE,
                "isee-basic/CAM01_0.conf");
        param.put(MessageHandlingApp.Parameter.WEBCAM_LOGIN_PARAM,
                new Gson().toJson(new LoginParam(InetAddress.getLocalHost(), 0,
                        "Ken Yu", "I love Shenzhen!")));

        for (String url : cam01VideoURLs) {
            param.put(MessageHandlingApp.Parameter.VIDEO_URL, url);

            sendWithLog(DataType.COMMAND.name(),
                    MessageHandlingApp.CommandType.TRACK_ONLY,
                    serialize(param),
                    producer,
                    logger);
            /*
            sendWithLog(DataType.COMMAND.name(),
                    MessageHandlingApp.CommandType.TRACK_ATTRRECOG,
                    serialize(param),
                    producer,
                    logger);

            sendWithLog(DataType.COMMAND.name(),
                    MessageHandlingApp.CommandType.TRACK_ATTRRECOG_REID,
                    serialize(param),
                    producer,
                    logger);
            */
        }
        /*
        param.put(MessageHandlingApp.Parameter.TRACKLET_INDEX, "1");

        sendWithLog(DataType.COMMAND.name(),
                MessageHandlingApp.CommandType.ATTRRECOG_ONLY,
                serialize(param),
                producer,
                logger);

        sendWithLog(DataType.COMMAND.name(),
                MessageHandlingApp.CommandType.ATTRRECOG_REID,
                serialize(param),
                producer,
                logger);

        sendWithLog(DataType.COMMAND.name(),
                MessageHandlingApp.CommandType.REID_ONLY,
                serialize(param),
                producer,
                logger);
        */
    }
}
