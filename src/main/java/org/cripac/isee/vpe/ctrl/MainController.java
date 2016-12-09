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

import org.apache.spark.launcher.SparkLauncher;
import org.apache.zookeeper.KeeperException.UnimplementedException;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter.NoAppSpecifiedException;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.*;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The MainController class initializes the system environment (currently only
 * checks the required topics), and provides entrance to run any spark
 * streaming applications.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class MainController {

    public static boolean listening = true;

    public static void main(String[] args)
            throws NoAppSpecifiedException,
            URISyntaxException,
            IOException,
            ParserConfigurationException,
            SAXException,
            UnimplementedException {
        // Analyze the command line and store the options into a system property
        // center.
        SystemPropertyCenter propCenter = new SystemPropertyCenter(args);

        // Prepare system configuration.
        if (propCenter.sparkMaster.toLowerCase().contains("yarn")) {
            System.setProperty("SPARK_YARN_MODE", "true");

            // If report listener is not specified by user, the terminal
            // starting this application is also responsible for listening
            // to runtime report.
            if (propCenter.reportListenerAddr == null) {
                // Create a UDP server for receiving reports.
                DatagramSocket server = new DatagramSocket(0);
                server.setSoTimeout(1000);

                // Create a thread to listen to reports.
                Thread listener = new Thread(() -> {
                    byte[] recvBuf = new byte[10000];
                    DatagramPacket recvPacket = new DatagramPacket(recvBuf, recvBuf.length);
                    while (listening) {
                        try {
                            server.receive(recvPacket);
                            String recvStr =
                                    new String(recvPacket.getData(), 0, recvPacket.getLength());
                            System.out.println(recvStr);
                        } catch (SocketTimeoutException e) {
                        } catch (IOException e) {
                            e.printStackTrace();
                            break;
                        }
                    }
                    System.out.println("[INFO]Stop listening!");
                    server.close();
                });
                listener.start();

                // Update the address and port of the report listener in the
                // property center.
                propCenter.reportListenerAddr = InetAddress.getLocalHost().getHostName();
                propCenter.reportListenerPort = server.getLocalPort();
            }

            String[] arguments = propCenter.getArgs();

            if (propCenter.verbose) {
                System.out.print("[INFO]Submitting with args:");
                for (String arg : arguments) {
                    System.out.print(" " + arg);
                }
                System.out.println("");
            }

            final class ProcessWithName {
                public Process process;
                public String name;

                public ProcessWithName(Process process, String name) {
                    this.process = process;
                    this.name = name;
                }
            }

            List<ProcessWithName> processesWithNames = new LinkedList<>();
            boolean useDefaultAppProperties = (propCenter.appPropFilePath == null);
            for (String appName : propCenter.appsToStart) {
                if (useDefaultAppProperties) {
                    propCenter.appPropFilePath = ConfManager.CONF_DIR + "/" + appName + "/app.properties";
                }

                SparkLauncher launcher = new SparkLauncher()
                        .setAppResource(propCenter.jarPath)
                        .setMainClass(AppManager.getMainClassName(appName))
                        .setMaster(propCenter.sparkMaster)
                        .setAppName(appName)
                        .setPropertiesFile(propCenter.sparkConfFilePath)
                        .setVerbose(propCenter.verbose)
                        .addFile(propCenter.log4jPropFilePath)
                        .addFile(propCenter.sysPropFilePath)
                        .addFile(ConfManager.getConcatCfgFilePathList(","))
                        .setConf(SparkLauncher.DRIVER_MEMORY, propCenter.driverMem)
                        .setConf(SparkLauncher.EXECUTOR_MEMORY, propCenter.executorMem)
                        .setConf(SparkLauncher.CHILD_PROCESS_LOGGER_NAME, appName)
                        .setConf(SparkLauncher.EXECUTOR_CORES, "" + propCenter.executorCores)
                        .addSparkArg("--driver-cores", "" + propCenter.driverCores)
                        .addSparkArg("--num-executors", "" + propCenter.numExecutors)
                        .addSparkArg("--total-executor-cores", "" + propCenter.totalExecutorCores)
                        .addSparkArg("--queue", propCenter.hadoopQueue)
                        .addAppArgs(propCenter.getArgs());
                if (new File(propCenter.appPropFilePath).exists()) {
                    launcher.addFile(propCenter.appPropFilePath);
                }

                Process launcherProcess = launcher.launch();
                processesWithNames.add(new ProcessWithName(launcherProcess, appName));

                // Create threads listening to output of the launcher process.
                Thread infoThread = new Thread(
                        new InputStreamReaderRunnable(launcherProcess.getInputStream(), "INFO"),
                        "LogStreamReader info");
                Thread errorThread = new Thread(
                        new InputStreamReaderRunnable(launcherProcess.getErrorStream(), "ERROR"),
                        "LogStreamReader error");
                infoThread.start();
                errorThread.start();
            }

            while (!processesWithNames.isEmpty()) {
                for (ProcessWithName processWithName : processesWithNames) {
                    try {
                        boolean exited = processWithName.process.waitFor(
                                100, TimeUnit.MILLISECONDS);
                        if (exited) {
                            System.out.println(
                                    "[INFO]Process " + processWithName.name
                                            + "finished! Exit code: "
                                            + processWithName.process.exitValue());
                            processesWithNames.remove(processWithName);
                            break;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            listening = false;
        } else {
            // TODO Complete code for running locally.
            throw new UnimplementedException();
        }
    }
}
