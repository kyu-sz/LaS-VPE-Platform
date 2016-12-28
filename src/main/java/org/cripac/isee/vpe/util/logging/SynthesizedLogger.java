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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.cripac.isee.vpe.common.DataTypes;
import org.cripac.isee.vpe.common.Topic;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;

import javax.annotation.Nonnull;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * The SynthesizedLogger class synthesizes various logging methods, like log4j,
 * raw console, Kafka... It welcomes modification by developers with their own
 * demands.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class SynthesizedLogger extends Logger {

    public static final Topic REPORT_TOPIC = new Topic("vpe_report", DataTypes.PLAIN_TEXT, null);

    private String username;
    private org.apache.log4j.Logger log4jLogger;
    private String localName;
    private KafkaProducer producer;

    /**
     * Create a synthesized logger specifying address and port to sendWithLog report
     * to.
     *
     * @param username   Name of the logger user.
     * @param propCenter Properties of the system.
     * @throws UnknownHostException
     * @throws SocketException
     */
    public SynthesizedLogger(@Nonnull String username,
                             @Nonnull SystemPropertyCenter propCenter) {
        super(propCenter.verbose ? Level.DEBUG : Level.INFO);

        this.username = username;

        PropertyConfigurator.configure("log4j.properties");
        log4jLogger = LogManager.getRootLogger();
        log4jLogger.setLevel(level);

        try {
            localName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            localName = "Unknown Host";
        }

        Properties producerProp = propCenter.generateKafkaProducerProp(true);
        producer = new KafkaProducer(producerProp);
    }

    private void send(@Nonnull String message) {
        producer.send(new ProducerRecord(REPORT_TOPIC.NAME, this.username, message));
    }

    public void debug(@Nonnull Object message) {
        if (Level.DEBUG.isGreaterOrEqual(level)) {
            log4jLogger.debug(message);
            String richMsg = "[DEBUG]\t" + localName + "\t" + username + ":\t" + message;
            System.out.println(richMsg);
            send(richMsg);
        }
    }

    public void debug(@Nonnull Object message,
                      @Nonnull Throwable t) {
        if (Level.DEBUG.isGreaterOrEqual(level)) {
            log4jLogger.debug(message, t);

            String richMsg = "[DEBUG]\t" + localName + "\t" + username + ":\t" + message + ": " + t;
            System.out.println(richMsg);
            t.printStackTrace();
            send(richMsg);

            String stackTraceMsg = "";
            StackTraceElement[] stackTrace = t.getStackTrace();
            for (StackTraceElement element : stackTrace) {
                stackTraceMsg = stackTraceMsg + element.toString() + "\n";
            }
            send(stackTraceMsg);
        }
    }

    public void info(@Nonnull Object message) {
        if (Level.INFO.isGreaterOrEqual(level)) {
            log4jLogger.info(message);
            String richMsg = "[INFO]\t" + localName + "\t" + username + ":\t" + message;
            System.out.println(richMsg);
            send(richMsg);
        }
    }

    public void info(@Nonnull Object message,
                     @Nonnull Throwable t) {
        if (Level.INFO.isGreaterOrEqual(level)) {
            log4jLogger.info(message, t);
            String richMsg = "[INFO]\t" + localName + "\t" + username + ":\t" + message + ": " + t;
            System.out.println(richMsg);
            t.printStackTrace();
            send(richMsg);
            String stackTraceMsg = "";
            StackTraceElement[] stackTrace = t.getStackTrace();
            for (StackTraceElement element : stackTrace) {
                stackTraceMsg = stackTraceMsg + element.toString() + "\n";
            }
            send(stackTraceMsg);
        }
    }

    public void warn(@Nonnull Object message) {
        if (Level.WARN.isGreaterOrEqual(level)) {
            log4jLogger.warn(message);
            String richMsg = "[WARNING]\t" + localName + "\t" + username + ":\t" + message;
            System.out.println(richMsg);
            send(richMsg);
        }
    }

    public void warn(@Nonnull Object message,
                     @Nonnull Throwable t) {
        if (Level.WARN.isGreaterOrEqual(level)) {
            log4jLogger.warn(message, t);

            String richMsg = "[WARNING]\t" + localName + "\t" + username + ":\t" + message + ": " + t;
            System.out.println(richMsg);
            t.printStackTrace();
            send(richMsg);

            String stackTraceMsg = "";
            StackTraceElement[] stackTrace = t.getStackTrace();
            for (StackTraceElement element : stackTrace) {
                stackTraceMsg = stackTraceMsg + element.toString() + "\n";
            }
            send(stackTraceMsg);
        }
    }

    public void error(@Nonnull Object message) {
        if (Level.ERROR.isGreaterOrEqual(level)) {
            log4jLogger.error(message);
            String richMsg = "[ERROR]\t" + localName + "\t" + username + ":\t" + message;
            System.err.println(richMsg);
            send(richMsg);
        }
    }

    public void error(@Nonnull Object message,
                      @Nonnull Throwable t) {
        if (Level.ERROR.isGreaterOrEqual(level)) {
            log4jLogger.error(message, t);

            String richMsg = "[ERROR]\t" + localName + "\t" + username + ":\t" + message + "\t" + t;
            System.err.println(richMsg);
            t.printStackTrace();
            send(richMsg);

            String stackTraceMsg = "";
            StackTraceElement[] stackTrace = t.getStackTrace();
            for (StackTraceElement element : stackTrace) {
                stackTraceMsg = stackTraceMsg + element.toString() + "\n";
            }
            send(stackTraceMsg);
        }
    }

    public void fatal(@Nonnull Object message) {
        if (Level.FATAL.isGreaterOrEqual(level)) {
            log4jLogger.fatal(message);
            String richMsg = "[FATAL]\t" + localName + "\t" + username + ":\t" + message;
            System.err.println(richMsg);
            send(richMsg);
        }
    }

    public void fatal(@Nonnull Object message,
                      @Nonnull Throwable t) {
        if (Level.FATAL.isGreaterOrEqual(level)) {
            log4jLogger.fatal(message, t);
            String richMsg = "[FATAL]\t" + localName + "\t" + username + ":\t" + message + ": " + t;
            System.err.println(richMsg);
            t.printStackTrace();
            send(richMsg);

            String stackTraceMsg = "";
            StackTraceElement[] stackTrace = t.getStackTrace();
            for (StackTraceElement element : stackTrace) {
                stackTraceMsg = stackTraceMsg + element.toString() + "\n";
            }
            send(stackTraceMsg);
        }
    }
}
