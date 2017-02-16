package org.cripac.isee.vpe.common;/***********************************************************************
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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.util.SerializationHelper;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.cripac.isee.vpe.util.SerializationHelper.serialize;
import static org.cripac.isee.vpe.util.kafka.KafkaHelper.sendWithLog;

/**
 * A Stream is a flow of DStreams. Each stream outputs at most one INPUT_TYPE of output.
 * <p>
 * Created by ken.yu on 16-10-26.
 */
public abstract class Stream implements Serializable {
    private static final long serialVersionUID = 7965952554107861881L;
    private final Singleton<KafkaProducer<String, byte[]>> producerSingleton;

    protected <T extends Serializable> void
    output(Collection<TaskData.ExecutionPlan.Node.Port> outputPorts,
           TaskData.ExecutionPlan executionPlan,
           T result,
           String taskID) throws Exception {
        final KafkaProducer<String, byte[]> producer = producerSingleton.getInst();
        for (TaskData.ExecutionPlan.Node.Port port : outputPorts) {
            final TaskData<T> resTaskData = new TaskData<>(port.getNode(), executionPlan, result);
            final byte[] serialized = serialize(resTaskData);
            sendWithLog(port.topic, taskID, serialized, producer, loggerSingleton.getInst());
        }
    }

    protected <T extends Serializable> JavaPairDStream<String, T>
    filter(JavaDStream<SparkStreamingApp.StringByteArrayRecord> stream, Topic topic) {
        return stream
                .filter(rec -> (Boolean) (rec.topic.equals(topic.NAME)))
                .mapToPair(rec -> {
                    try {
                        return new Tuple2<>(rec.key, SerializationHelper.<T>deserialize(rec.value));
                    } catch (Exception e) {
                        loggerSingleton.getInst().error("On deserialization", e);
                        return null;
                    }
                });
    }

    protected final Singleton<Logger> loggerSingleton;

    /**
     * Initialize necessary components of a Stream object.
     *
     * @param appName    Enclosing application name.
     * @param propCenter System property center.
     * @throws Exception On failure creating singleton.
     */
    public Stream(String appName, SystemPropertyCenter propCenter) throws Exception {
        this.loggerSingleton = new Singleton<>(new SynthesizedLoggerFactory(appName, propCenter));

        Properties producerProp = propCenter.getKafkaProducerProp(false);
        producerSingleton = new Singleton<>(new KafkaProducerFactory<String, byte[]>(producerProp));
    }

    /**
     * Append the stream to a Spark Streaming stream.
     *
     * @param globalStream A Spark Streaming stream.
     */
    public abstract void addToStream(JavaDStream<SparkStreamingApp.StringByteArrayRecord> globalStream);

    public abstract List<String> listeningTopics();
}
