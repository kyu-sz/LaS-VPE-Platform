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

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.zookeeper.KeeperException.UnimplementedException;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter.NoAppSpecifiedException;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The MainController class provides an entrance to run any spark
 * streaming applications, and listen to their reports.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class MainController {

    public static void main(String[] args)
            throws URISyntaxException, IOException, ParserConfigurationException, SAXException, UnimplementedException {
        // Analyze the command line and store the options into a system property
        // center.
        SystemPropertyCenter propCenter = new SystemPropertyCenter(args);

        final AtomicReference<Boolean> running = new AtomicReference<>();
        running.set(true);

        // Prepare system configuration.
        if (propCenter.sparkMaster.toLowerCase().contains("yarn")) {
            System.setProperty("SPARK_YARN_MODE", "true");

            // Create a thread to listen to reports.
            Thread listener = new Thread(() -> {
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                        propCenter.getKafkaConsumerProp(UUID.randomUUID().toString(), true));
                ArrayList<String> topicList = new ArrayList<>();
                for (String appName : propCenter.appsToStart) {
                    topicList.add(appName + "_report");
                }
                consumer.subscribe(topicList);
                while (running.get()) {
                    try {
                        ConsumerRecords<String, String> records = consumer.poll(propCenter.batchDuration);
                        records.forEach(rec -> System.out.println(rec.value()));
                        consumer.commitSync();
                    } catch (Exception | NoClassDefFoundError e) {
                        e.printStackTrace();
                        // Do nothing and try again.
                    }
                }
            });
            listener.start();

            final class ProcessWithName {
                private Process process;
                private String name;

                private ProcessWithName(Process process, String name) {
                    this.process = process;
                    this.name = name;
                }
            }

            List<ProcessWithName> processesWithNames = new LinkedList<>();
            for (String appName : propCenter.appsToStart) {
                try {
                    SparkLauncher launcher = propCenter.GetSparkLauncher(appName);

                    Process launcherProcess = launcher.launch();
                    processesWithNames.add(new ProcessWithName(launcherProcess, appName));

                    // Create threads listening to output of the launcher process.
                    Thread infoThread = new Thread(
                            new InputStreamReaderRunnable(launcherProcess.getInputStream(), "INFO", running),
                            "LogStreamReader info");
                    Thread errorThread = new Thread(
                            new InputStreamReaderRunnable(launcherProcess.getErrorStream(), "ERROR", running),
                            "LogStreamReader error");
                    infoThread.start();
                    errorThread.start();
                } catch (NoAppSpecifiedException e) {
                    e.printStackTrace();
                }
            }

            while (!processesWithNames.isEmpty()) {
                for (ProcessWithName processWithName : processesWithNames) {
                    try {
                        boolean exited = processWithName.process.waitFor(100, TimeUnit.MILLISECONDS);
                        if (exited) {
                            System.out.println("[INFO]Process " + processWithName.name + "finished! Exit code: "
                                    + processWithName.process.exitValue());
                            processWithName.process.getInputStream().close();
                            processWithName.process.getErrorStream().close();
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

        running.set(false);
    }
}
