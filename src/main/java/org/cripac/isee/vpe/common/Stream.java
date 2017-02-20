package org.cripac.isee.vpe.common;/*
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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A Stream is a flow of DStreams. Each stream outputs at most one inputType of output.
 * <p>
 * Created by ken.yu on 16-10-26.
 */
public abstract class Stream implements Serializable {
    private static final long serialVersionUID = 7965952554107861881L;
    private final Singleton<KafkaProducer<String, byte[]>> producerSingleton;

    protected void
    output(Collection<TaskData.ExecutionPlan.Node.Port> outputPorts,
           TaskData.ExecutionPlan executionPlan,
           Serializable result,
           String taskID) throws Exception {
        KafkaHelper.sendWithLog(taskID,
                new TaskData(outputPorts, executionPlan, result),
                producerSingleton.getInst(),
                loggerSingleton.getInst());
    }

    protected JavaPairDStream<String, TaskData>
    filter(Map<String, JavaPairDStream<String, TaskData>> streamMap, Port port) {
        return streamMap.get(port.inputType.name())
                .filter(rec -> (Boolean) rec._2().destPorts.containsKey(port));
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
     * Add streaming actions to the global {@link TaskData} stream.
     * This global stream contains pre-deserialized TaskData messages, so as to save time.
     *
     * @param globalStreamMap A map of streams. The key of an entry is the topic name,
     *                        which must be one of the {@link DataType}.
     *                        The value is a filtered stream.
     */
    public abstract void addToGlobalStream(Map<String, JavaPairDStream<String, TaskData>> globalStreamMap);

    /**
     * Get input ports of the stream.
     *
     * @return A list of ports.
     */
    public abstract List<Port> getPorts();

    /**
     * The class Port represents an input port of a stream.
     */
    public static final class Port implements Serializable {
        private static final long serialVersionUID = -7567029992452814611L;
        /**
         * Name of the prototype to appear in Kafka.
         */
        public final String name;
        /**
         * Type of the prototype used within the system.
         */
        public final DataType inputType;

        /**
         * Create a prototype.
         *
         * @param name Name of the prototype to appear in Kafka.
         * @param type Type of the prototype used within the system.
         */
        public Port(@Nonnull String name,
                    @Nonnull DataType type) {
            this.name = name;
            this.inputType = type;
        }

        /**
         * Transform the prototype into a string in format as "[inputType]name".
         *
         * @return String representing the prototype.
         */
        @Override
        public String toString() {
            return "[" + inputType + "]" + name;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }
}
