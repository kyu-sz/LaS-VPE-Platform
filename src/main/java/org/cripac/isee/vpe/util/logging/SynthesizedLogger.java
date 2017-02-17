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

package org.cripac.isee.vpe.util.logging;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;

import javax.annotation.Nonnull;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The SynthesizedLogger class synthesizes various logging methods, like log4j,
 * raw console, Kafka... It welcomes modification by developers with their own
 * demands.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class SynthesizedLogger extends Logger {

    private final String username;
    private final String reportTopic;
    private org.apache.log4j.Logger log4jLogger;
    private ConsoleLogger consoleLogger;
    private KafkaProducer<String, String> producer;

    private final static SimpleDateFormat ft = new SimpleDateFormat("yy.MM.dd HH:mm:ss");

    private String wrapMsg(Object msg) {
        return ft.format(new Date()) + "\t" + localName + "\t" + username + ":\t" + msg;
    }

    private void checkTopic(String topic, SystemPropertyCenter propCenter) {
        ZkConnection zkConn = new ZkConnection(propCenter.zkConn, propCenter.sessionTimeoutMs);
        ZkClient zkClient = new ZkClient(zkConn);
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConn, false);
        if (!AdminUtils.topicExists(zkUtils, topic)) {
            // AdminUtils.createTopic(zkClient, topic,
            // propCenter.kafkaNumPartitions,
            // propCenter.kafkaReplFactor, new Properties());
            kafka.admin.TopicCommand.main(
                    new String[]{
                            "--create",
                            "--zookeeper", propCenter.zkConn,
                            "--topic", topic,
                            "--partitions", "" + propCenter.kafkaNumPartitions,
                            "--replication-factor", "" + propCenter.kafkaReplFactor});
        }
    }

    /**
     * Create a synthesized logger. Logs will be print to console, transferred to default Log4j logger and sent to
     * Kafka on topic ${username}_report.
     *
     * @param username   Name of the logger user.
     * @param propCenter Properties of the system.
     */
    public SynthesizedLogger(@Nonnull String username,
                             @Nonnull SystemPropertyCenter propCenter) {
        super(propCenter.verbose ? Level.DEBUG : Level.INFO);

        this.username = username;
        this.reportTopic = username + "_report";

        PropertyConfigurator.configure("log4j.properties");
        log4jLogger = LogManager.getRootLogger();
        log4jLogger.setLevel(level);

        consoleLogger = new ConsoleLogger(this.level);

        checkTopic(reportTopic, propCenter);
        Properties producerProp = propCenter.getKafkaProducerProp(true);
        producer = new KafkaProducer<>(producerProp);
    }

    @Override
    protected void finalize() throws Throwable {
        producer.close();
        super.finalize();
    }

    private void send(@Nonnull String message) {
        Future<RecordMetadata> metadataFuture =
                producer.send(new ProducerRecord<>(reportTopic, username, message));
        try {
            RecordMetadata recordMetadata = metadataFuture.get(5, TimeUnit.SECONDS);
            consoleLogger.debug("Report sent to " + recordMetadata.topic() + ":" + recordMetadata.partition()
                    + "-" + recordMetadata.offset());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            consoleLogger.error("Error on sending report", e);
        }
    }

    public void debug(@Nonnull Object message) {
        if (Level.DEBUG.isGreaterOrEqual(level)) {
            log4jLogger.debug(message);
            consoleLogger.debug(message);
            String richMsg = "[DEBUG]\t" + wrapMsg(message);
            send(richMsg);
        }
    }

    public void debug(@Nonnull Object message,
                      @Nonnull Throwable t) {
        if (Level.DEBUG.isGreaterOrEqual(level)) {
            log4jLogger.debug(message, t);
            consoleLogger.debug(message, t);

            String richMsg = "[DEBUG]\t" + wrapMsg(message) + ": " + t;
            send(richMsg);

            String stackTraceMsg = "";
            StackTraceElement[] stackTrace = t.getStackTrace();
            for (StackTraceElement element : stackTrace) {
                stackTraceMsg = stackTraceMsg + "\t" + element.toString() + "\n";
            }
            send(stackTraceMsg);
        }
    }

    public void info(@Nonnull Object message) {
        if (Level.INFO.isGreaterOrEqual(level)) {
            log4jLogger.info(message);
            consoleLogger.info(message);
            String richMsg = "[INFO]\t" + wrapMsg(message);
            send(richMsg);
        }
    }

    public void info(@Nonnull Object message,
                     @Nonnull Throwable t) {
        if (Level.INFO.isGreaterOrEqual(level)) {
            log4jLogger.info(message, t);
            consoleLogger.info(message, t);
            String richMsg = "[INFO]\t" + wrapMsg(message) + ": " + t;
            send(richMsg);
            String stackTraceMsg = "";
            StackTraceElement[] stackTrace = t.getStackTrace();
            for (StackTraceElement element : stackTrace) {
                stackTraceMsg = stackTraceMsg + "\t" + element.toString() + "\n";
            }
            send(stackTraceMsg);
        }
    }

    public void warn(@Nonnull Object message) {
        if (Level.WARN.isGreaterOrEqual(level)) {
            log4jLogger.warn(message);
            consoleLogger.warn(message);
            String richMsg = "[WARNING]\t" + wrapMsg(message);
            send(richMsg);
        }
    }

    public void warn(@Nonnull Object message,
                     @Nonnull Throwable t) {
        if (Level.WARN.isGreaterOrEqual(level)) {
            log4jLogger.warn(message, t);
            consoleLogger.warn(message, t);

            String richMsg = "[WARNING]\t" + wrapMsg(message) + ": " + t;
            send(richMsg);

            String stackTraceMsg = "";
            StackTraceElement[] stackTrace = t.getStackTrace();
            for (StackTraceElement element : stackTrace) {
                stackTraceMsg = stackTraceMsg + "\t" + element.toString() + "\n";
            }
            send(stackTraceMsg);
        }
    }

    public void error(@Nonnull Object message) {
        if (Level.ERROR.isGreaterOrEqual(level)) {
            log4jLogger.error(message);
            consoleLogger.error(message);
            String richMsg = "[ERROR]\t" + wrapMsg(message);
            send(richMsg);
        }
    }

    public void error(@Nonnull Object message,
                      @Nonnull Throwable t) {
        if (Level.ERROR.isGreaterOrEqual(level)) {
            log4jLogger.error(message, t);
            consoleLogger.error(message, t);

            String richMsg = "[ERROR]\t" + wrapMsg(message) + "\t" + t;
            send(richMsg);

            String stackTraceMsg = "";
            StackTraceElement[] stackTrace = t.getStackTrace();
            for (StackTraceElement element : stackTrace) {
                stackTraceMsg = stackTraceMsg + "\t" + element.toString() + "\n";
            }
            send(stackTraceMsg);
        }
    }

    public void fatal(@Nonnull Object message) {
        if (Level.FATAL.isGreaterOrEqual(level)) {
            log4jLogger.fatal(message);
            consoleLogger.fatal(message);
            String richMsg = "[FATAL]\t" + wrapMsg(message);
            send(richMsg);
        }
    }

    public void fatal(@Nonnull Object message,
                      @Nonnull Throwable t) {
        if (Level.FATAL.isGreaterOrEqual(level)) {
            log4jLogger.fatal(message, t);
            consoleLogger.fatal(message, t);
            String richMsg = "[FATAL]\t" + wrapMsg(message) + ": " + t;
            send(richMsg);

            String stackTraceMsg = "";
            StackTraceElement[] stackTrace = t.getStackTrace();
            for (StackTraceElement element : stackTrace) {
                stackTraceMsg = stackTraceMsg + "\t" + element.toString() + "\n";
            }
            send(stackTraceMsg);
        }
    }
}
