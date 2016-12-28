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

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Level;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.zookeeper.KeeperException.UnimplementedException;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter.NoAppSpecifiedException;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.cripac.isee.vpe.util.logging.SynthesizedLogger.REPORT_TOPIC;

/**
 * The MainController class initializes the system environment (currently only
 * checks the required topics), and provides entrance to run any spark
 * streaming applications.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class MainController {

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

            // Create a thread to listen to reports.
            Thread listener = new Thread(() -> {
                Logger logger = new ConsoleLogger(Level.DEBUG);
                KafkaConsumer consumer = new KafkaConsumer(
                        propCenter.generateKafkaConsumerProp(UUID.randomUUID().toString(), true));
                consumer.subscribe(Arrays.asList(REPORT_TOPIC.NAME));
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(0);
                    records.forEach(rec -> logger.info(rec.key() + ":\t" + rec.value()));
                }
            });
            listener.start();

            final class ProcessWithName {
                public Process process;
                public String name;

                public ProcessWithName(Process process, String name) {
                    this.process = process;
                    this.name = name;
                }
            }

            List<ProcessWithName> processesWithNames = new LinkedList<>();
            for (String appName : propCenter.appsToStart) {
                SparkLauncher launcher = propCenter.GetLauncher(appName);

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
                            System.out.println("[INFO]Process " + processWithName.name + "finished! Exit code: "
                                    + processWithName.process.exitValue());
                            processesWithNames.remove(processWithName);
                            break;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            // TODO Complete code for running locally.
            throw new UnimplementedException();
        }
        System.exit(0);
    }
}
