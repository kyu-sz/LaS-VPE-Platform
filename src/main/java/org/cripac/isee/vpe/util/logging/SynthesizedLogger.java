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

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.*;

/**
 * The SynthesizedLogger class synthesizes various logging methods, like log4j,
 * raw console, socket... It welcomes modification by developers with their own
 * demands.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class SynthesizedLogger extends Logger {

    private String appName;
    private org.apache.log4j.Logger log4jLogger;
    private DatagramSocket sender;
    private InetAddress listenerAddr;
    private String localName;
    private int listenerPort;

    /**
     * Create a synthesized logger specifying address and port to sendWithLog report
     * to.
     *
     * @param appName            Name of the application using the logger.
     * @param level              Level of logging.
     * @param reportListenerAddr Address of server listening to report.
     * @param reportListenerAddr Port of server listening to report.
     * @throws UnknownHostException
     * @throws SocketException
     */
    public SynthesizedLogger(@Nonnull String appName,
                             @Nonnull Level level,
                             @Nonnull String reportListenerAddr,
                             int reportListenerPort) {
        super(level);

        this.appName = appName;

        PropertyConfigurator.configure("log4j.properties");
        log4jLogger = LogManager.getRootLogger();
        log4jLogger.setLevel(level);

        try {
            localName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            localName = "Unknown Host";
        }

        try {
            sender = new DatagramSocket();
        } catch (SocketException e) {
            e.printStackTrace();
            sender = null;
        }

        try {
            listenerAddr = InetAddress.getByName(reportListenerAddr);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            listenerAddr = null;
            sender = null;
        }

        listenerPort = reportListenerPort;
    }

    private void send(@Nonnull String message) {
        if (sender != null) {
            byte[] sendBuf = message.getBytes();
            DatagramPacket sendPacket =
                    new DatagramPacket(sendBuf, sendBuf.length, listenerAddr, listenerPort);
            try {
                sender.send(sendPacket);
            } catch (IOException e) {
                System.err.println(
                        "[ERROR]" + localName + "\t" + appName
                                + ":\tError occurred when reporting to "
                                + listenerAddr + ":" + listenerPort);
                e.printStackTrace();
                sender = null;
            }
        } else {
            System.err.println("[ERROR]" + localName + "\t" + appName + ":\tSender dead!");
        }
    }

    public void debug(@Nonnull Object message) {
        if (Level.DEBUG.isGreaterOrEqual(level)) {
            log4jLogger.debug(message);
            String richMsg = "[DEBUG]" + localName + "\t" + appName + ":\t" + message;
            System.out.println(richMsg);
            send(richMsg);
        }
    }

    public void debug(@Nonnull Object message,
                      @Nonnull Throwable t) {
        if (Level.DEBUG.isGreaterOrEqual(level)) {
            log4jLogger.debug(message, t);

            String richMsg = "[DEBUG]" + localName + "\t" + appName + ":\t" + message + t;
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
            String richMsg = "[INFO]" + localName + "\t" + appName + ":\t" + message;
            System.out.println(richMsg);
            send(richMsg);
        }
    }

    public void info(@Nonnull Object message,
                     @Nonnull Throwable t) {
        if (Level.INFO.isGreaterOrEqual(level)) {
            log4jLogger.info(message, t);
            String richMsg = "[INFO]" + localName + "\t" + appName + ":\t" + message + t;
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
            String richMsg = "[WARN]" + localName + "\t" + appName + ":\t" + message;
            System.out.println(richMsg);
            send(richMsg);
        }
    }

    public void warn(@Nonnull Object message,
                     @Nonnull Throwable t) {
        if (Level.WARN.isGreaterOrEqual(level)) {
            log4jLogger.warn(message, t);

            String richMsg = "[WARN]" + localName + "\t" + appName + ":\t" + message + t;
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
            String richMsg = "[ERROR]" + localName + "\t" + appName + ":\t" + message;
            System.err.println(richMsg);
            send(richMsg);
        }
    }

    public void error(@Nonnull Object message,
                      @Nonnull Throwable t) {
        if (Level.ERROR.isGreaterOrEqual(level)) {
            log4jLogger.error(message, t);

            String richMsg = "[ERROR]" + localName + "\t" + appName + ":\t" + message + "\t" + t;
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
            String richMsg = "[FATAL]" + localName + "\t" + appName + ":\t" + message;
            System.err.println(richMsg);
            send(richMsg);
        }
    }

    public void fatal(@Nonnull Object message,
                      @Nonnull Throwable t) {
        if (Level.FATAL.isGreaterOrEqual(level)) {
            log4jLogger.fatal(message, t);
            String richMsg = "[FATAL]" + localName + "\t" + appName + ":\t" + message + t;
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
