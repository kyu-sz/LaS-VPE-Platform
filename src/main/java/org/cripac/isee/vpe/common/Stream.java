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
package org.cripac.isee.vpe.common;

import kafka.common.FailedToSendMessageException;
import kafka.common.MessageSizeTooLargeException;
import org.apache.kafka.common.KafkaException;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.vpe.util.kafka.ByteArrayProducer;
import org.cripac.isee.vpe.util.kafka.ByteArrayProducerFactory;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.*;

/**
 * A Stream is a flow of DStreams. Each stream outputs at most one type of data.
 * <p>
 * Created by ken.yu on 16-10-26.
 */
public abstract class Stream implements Serializable {
    private static final long serialVersionUID = 7965952554107861881L;
    private final Singleton<ByteArrayProducer> producerSingleton;
    private final boolean verbose;

    protected void
    output(Collection<TaskData.ExecutionPlan.Node.Port> outputPorts,
           TaskData.ExecutionPlan executionPlan,
           Serializable result,
           UUID taskID) throws Exception {
        new RobustExecutor<Void, Void>(
                () -> {
                    if (verbose) {
                        KafkaHelper.sendWithLog(taskID.toString(),
                                new TaskData(outputPorts, executionPlan, result),
                                producerSingleton.getInst(),
                                loggerSingleton.getInst());
                    } else {
                        KafkaHelper.send(taskID.toString(),
                                new TaskData(outputPorts, executionPlan, result),
                                producerSingleton.getInst());
                    }
                },
                Arrays.asList(
                        MessageSizeTooLargeException.class,
                        KafkaException.class,
                        FailedToSendMessageException.class)
        ).execute();
    }

    protected JavaPairDStream<UUID, TaskData>
    filter(Map<DataType, JavaPairDStream<UUID, TaskData>> streamMap, Port port) {
        return streamMap.get(port.inputType)
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
        this.verbose = propCenter.verbose;

        this.loggerSingleton = new Singleton<>(new SynthesizedLoggerFactory(appName, propCenter), SynthesizedLogger.class);

        Properties producerProp = propCenter.getKafkaProducerProp(false);
        producerSingleton = new Singleton<>(new ByteArrayProducerFactory(producerProp), ByteArrayProducer.class);
    }

    /**
     * Add streaming actions to the global {@link TaskData} stream.
     * This global stream contains pre-deserialized TaskData messages, so as to save time.
     *
     * @param globalStreamMap A map of streams. The key of an entry is the topic name,
     *                        which must be one of the {@link DataType}.
     *                        The value is a filtered stream.
     */
    public abstract void addToGlobalStream(Map<DataType, JavaPairDStream<UUID, TaskData>> globalStreamMap);

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
         * Name of the port.
         */
        public final String name;
        /**
         * Input data type of the port.
         */
        public final DataType inputType;

        /**
         * Create a port.
         *
         * @param name Name of the port.
         * @param type Input data type of the port.
         */
        public Port(@Nonnull String name,
                    @Nonnull DataType type) {
            this.name = name;
            this.inputType = type;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof Port) ? name.equals(((Port) o).name) : super.equals(o);
        }
    }
}
