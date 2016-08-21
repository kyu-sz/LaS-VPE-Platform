/***********************************************************************
 * This file is part of VPE-Platform.
 * 
 * VPE-Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * VPE-Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with VPE-Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/

package org.casia.cripac.isee.vpe.common;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.casia.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.casia.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.casia.cripac.isee.vpe.ctrl.MessageHandlingApp;
import org.casia.cripac.isee.vpe.ctrl.MetadataSavingApp;
import org.casia.cripac.isee.vpe.debug.CommandGeneratingApp;
import org.xml.sax.SAXException;

/**
 * The SystemPropertyCenter class is responsible of managing the properties of
 * the systems. There are some properties predefined, and they can be
 * overwritten by command options or an extern property file. It can also
 * generate back command options for uses like SparkSubmit.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class SystemPropertyCenter {

	/**
	 * Thrown when no application is specified in any possible property sources.
	 * 
	 * @author Ken Yu, CRIPAC, 2016
	 *
	 */
	public static class NoAppSpecifiedException extends Exception {
		private static final long serialVersionUID = -8356206863229009557L;
	}

	// Zookeeper properties
	public String zookeeperConnect = "localhost:2181";
	public int sessionTimeoutMs = 10 * 10000;
	public int connectionTimeoutMs = 8 * 1000;

	// Kafka properties
	public String kafkaBrokers = "localhost:9092";
	public int kafkaPartitions = 1;
	public int kafkaReplicationFactor = 1;
	public int kafkaFetchMessageMaxBytes = 10000000;

	// Spark properties
	public String checkpointRootDir = "checkpoint";
	public String metadataDir = "metadata";
	public String sparkMaster = "local[*]";
	public String sparkDeployMode = "client";
	public String appName = "";
	public boolean onYARN = false;
	public String executorMem = "1G"; // Memory per executor (e.g. 1000M, 2G)
										// (Default: 1G)
	public int numExecutors = 2; // Number of executors to start (Default: 2)
	public int executorCores = 1; // Number of cores per executor (Default: 1)
	public String driverMem = "1G"; // Memory for driver (e.g. 1000M, 2G)
									// (Default: 1024 Mb)
	public int driverCores = 1; // Number of cores used by the driver (Default:
								// 1).
	public String hadoopQueue = "default"; // The hadoop queue to use for
											// allocation requests (Default:
											// 'default')

	public String systemPropertiesFilePath = "conf/system.properties";
	public String sparkConfFilePath = "conf/spark-defaults.conf";
	public String log4jPropertiesFilePath = "conf/log4j.properties";
	public String hdfsDefaultName = "localhost:9000";
	public String yarnResourceManagerHostname = "localhost";
	public String jarPath = "bin/vpe-platform.jar";
	public int numRecvStreams = 5;

	public String messageListenerAddress = "localhost";
	public int messageListenerPort = 0;

	/**
	 * Whether to print verbose running information.
	 */
	public boolean verbose = false;

	/**
	 * Construction function supporting allocating a SystemPropertyCenter then
	 * filling in the properties manually.
	 */
	public SystemPropertyCenter() {
	}

	/**
	 * Generate command line options for SparkSubmit client, according to the
	 * stored properties.
	 * 
	 * @return An array of string with format required by SparkSubmit client.
	 * @throws NoAppSpecifiedException
	 */
	public String[] getArgs() throws NoAppSpecifiedException {
		ArrayList<String> options = new ArrayList<>();

		if (verbose) {
			options.add("-v");
		}

		options.add("--system-properties-file");
		if (onYARN) {
			options.add("system.properties");
		} else {
			options.add(systemPropertiesFilePath);
		}

		options.add("--log4j-properties-file");
		if (onYARN) {
			options.add("log4j.properties");
		} else {
			options.add(log4jPropertiesFilePath);
		}

		options.add("--message-listening-addr");
		options.add(messageListenerAddress);

		options.add("--message-listening-port");
		options.add("" + messageListenerPort);

		return Arrays.copyOf(options.toArray(), options.size(), String[].class);
	}

	public SystemPropertyCenter(String[] args) throws URISyntaxException, ParserConfigurationException, SAXException {

		CommandLineParser parser = new BasicParser();
		Options options = new Options();
		options.addOption("v", "verbose", false, "Display debug information.");
		options.addOption("a", "application", true, "Application specified to run.");
		options.addOption(null, "spark-properties-file", true, "File path of the spark property file.");
		options.addOption(null, "system-properties-file", true, "File path of the system property file.");
		options.addOption(null, "log4j-properties-file", true, "File path of the log4j property file.");
		options.addOption(null, "message-listening-addr", true, "");
		options.addOption(null, "message-listening-port", true, "");
		CommandLine commandLine;

		try {
			commandLine = parser.parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			System.out.println("Try using '-h' for more information.");
			System.exit(0);
			return;
		}

		if (commandLine.hasOption('v')) {
			System.out.println("Verbosity enabled!");
			verbose = true;
		}

		if (commandLine.hasOption('a')) {
			appName = commandLine.getOptionValue('a');
			if (verbose) {
				System.out.println("To start application " + appName + "...");
			}
		}

		if (commandLine.hasOption("system-properties-file")) {
			systemPropertiesFilePath = commandLine.getOptionValue("system-properties-file");
		}

		if (commandLine.hasOption("log4j-properties-file")) {
			log4jPropertiesFilePath = commandLine.getOptionValue("log4j-properties-file");
		}

		if (commandLine.hasOption("spark-properties-file")) {
			sparkConfFilePath = commandLine.getOptionValue("spark-properties-file");
		}

		// Load the property file.
		Properties systemProperties = new Properties();
		BufferedInputStream propInputStream;
		try {
			if (systemPropertiesFilePath.contains("hdfs:/")) {
				if (verbose) {
					System.out
							.println("Loading properties using HDFS platform from " + systemPropertiesFilePath + "...");
				}

				FileSystem fileSystem = FileSystem.get(new URI(systemPropertiesFilePath), HadoopUtils.getDefaultConf());
				FSDataInputStream hdfsInputStream = fileSystem.open(new Path(systemPropertiesFilePath));
				propInputStream = new BufferedInputStream(hdfsInputStream);
			} else {
				if (verbose) {
					System.out.println("Loading properties locally from " + systemPropertiesFilePath + "...");
				}

				propInputStream = new BufferedInputStream(new FileInputStream(systemPropertiesFilePath));
			}
			systemProperties.load(propInputStream);
		} catch (IOException e) {
			e.printStackTrace();
			System.err.printf("Cannot load system property file at specified path: \"%s\"!\n",
					systemPropertiesFilePath);
			System.out.println("Try use '-h' for more information.");
			System.exit(0);
			return;
		}

		// Digest the settings.
		for (Entry<Object, Object> entry : systemProperties.entrySet()) {
			if (verbose) {
				System.out.println("Read from property file: " + entry.getKey() + "=" + entry.getValue());
			}
			switch ((String) entry.getKey()) {
			case "zookeeper.connect":
				zookeeperConnect = (String) entry.getValue();
				break;
			case "kafka.brokers":
				kafkaBrokers = (String) entry.getValue();
				break;
			case "kafka.partitions":
				kafkaPartitions = new Integer((String) entry.getValue());
				break;
			case "kafka.replication.factor":
				kafkaReplicationFactor = new Integer((String) entry.getValue());
				break;
			case "kafka.fetch.message.max.bytes":
				kafkaFetchMessageMaxBytes = new Integer((String) entry.getValue());
				break;
			case "spark.checkpoint.dir":
				checkpointRootDir = (String) entry.getValue();
				break;
			case "vpe.metadata.dir":
				metadataDir = (String) entry.getValue();
				break;
			case "spark.master":
				sparkMaster = (String) entry.getValue();
				break;
			case "spark.deploy.mode":
				sparkDeployMode = (String) entry.getValue();
				break;
			case "vpe.platform.jar":
				jarPath = (String) entry.getValue();
				break;
			case "yarn.resource.manager.hostname":
				yarnResourceManagerHostname = (String) entry.getValue();
				break;
			case "hdfs.default.name":
				hdfsDefaultName = (String) entry.getValue();
				break;
			case "executor.num":
				numExecutors = new Integer((String) entry.getValue());
				break;
			case "executor.memory":
				executorMem = (String) entry.getValue();
				break;
			case "executor.cores":
				executorCores = new Integer((String) entry.getValue());
				break;
			case "driver.memory":
				driverMem = (String) entry.getValue();
				break;
			case "driver.cores":
				driverCores = new Integer((String) entry.getValue());
				break;
			case "hadoop.queue":
				hadoopQueue = (String) entry.getValue();
				break;
			case "vpe.recv.parallel":
				numRecvStreams = new Integer((String) entry.getValue());
				break;
			}
		}

		if (commandLine.hasOption("message-listening-addr")) {
			messageListenerAddress = commandLine.getOptionValue("message-listening-addr");
		}
		if (commandLine.hasOption("message-listening-port")) {
			messageListenerPort = new Integer(commandLine.getOptionValue("message-listening-port"));
		}

		if (sparkMaster.contains("yarn") && !onYARN) {
			onYARN = true;
			if (verbose) {
				System.out.println("To run on YARN...");
			}
		}
	}

	public String getMainClass() throws NoAppSpecifiedException {
		switch (appName) {
		case MessageHandlingApp.APP_NAME:
			return "org.casia.cripac.isee.vpe.ctrl.MessageHandlingApp";
		case MetadataSavingApp.APP_NAME:
			return "org.casia.cripac.isee.vpe.ctrl.MetadataSavingApp";
		case PedestrianAttrRecogApp.APP_NAME:
			return "org.casia.cripac.isee.vpe.alg.PedestrianAttrRecogApp";
		case PedestrianTrackingApp.APP_NAME:
			return "org.casia.cripac.isee.vpe.alg.PedestrianTrackingApp";
		case CommandGeneratingApp.APPLICATION_NAME:
			return "org.casia.cripac.isee.vpe.debug.CommandGeneratingApp";
		default:
			System.err.printf("No application named \"%s\"!\n", appName);
		case "":
			System.out.println("Try using '-h' for more information.");
			throw new NoAppSpecifiedException();
		}
	}
}
