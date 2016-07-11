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

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.casia.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.casia.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.casia.cripac.isee.vpe.ctrl.MessageHandlingApp;
import org.casia.cripac.isee.vpe.ctrl.MetadataSavingApp;
import org.casia.cripac.isee.vpe.debug.CommandGeneratingApp;

public class SystemPropertyCenter {
	
	public static class NoAppSpecifiedException extends Exception {
		private static final long serialVersionUID = -8356206863229009557L;
	}
	
	//Zookeeper properties
	public String zookeeper = "localhost:2181";
	public int sessionTimeoutMs = 10 * 10000;
	public int connectionTimeoutMs = 8 * 1000;
	
	//Kafka properties
	public String kafkaBrokers = "localhost:9092";
	public int kafkaPartitions = 1;
	public int kafkaReplicationFactor = 1;
	
	//Spark properties
	public String checkpointDir = "checkpoint";
	public String sparkMaster = "local[*]";
	public String sparkDeployMode = "client";
	public String applicationName = "";
	public boolean onYARN = false;
	
	public String propertyFilePath = "";
	public String hdfsNameNode = "localhost:9000";
	public String yarnResourceManagerHostname = "localhost";
	public String jarPath = "bin/vpe-platform.jar";
	
	public boolean verbose = false;
	
	public SystemPropertyCenter() { }
	
	public String[] generateCommandLineOpts() throws NoAppSpecifiedException {
		ArrayList<String> options = new ArrayList<>();

		if (onYARN) {
			options.add("--name");
			options.add(applicationName);
	
			//Determine which application to start.
			options.add("--class");
			switch (applicationName) {
			case MessageHandlingApp.APPLICATION_NAME:
				options.add("org.casia.cripac.isee.vpe.ctrl.MessageHandlingApp");
				break;
			case MetadataSavingApp.APPLICATION_NAME:
				options.add("org.casia.cripac.isee.vpe.ctrl.MetadataSavingApp");
				break;
			case PedestrianAttrRecogApp.APPLICATION_NAME:
				options.add("org.casia.cripac.isee.vpe.alg.PedestrianAttrRecogApp");
				break;
			case PedestrianTrackingApp.APPLICATION_NAME:
				options.add("org.casia.cripac.isee.vpe.alg.PedestrianTrackingApp");
				break;
			case CommandGeneratingApp.APPLICATION_NAME:
				options.add("org.casia.cripac.isee.vpe.debug.CommandGeneratingApp");
				break;
			default:
				System.err.printf("No application named \"%s\"!\n", applicationName);
			case "":
				System.out.println("Try using '-h' for more information.");
				throw new NoAppSpecifiedException();
			}
	
	//		options.add("--master");
	//		options.add(sparkMaster);
			
	//		options.add("--deploy-mode");
	//		options.add(sparkDeployMode);
	
			options.add("--jar");
			options.add(jarPath);
	
			options.add("--arg");
			if (verbose) {
				options.add("-v");
			}
	
			options.add("--arg");
			options.add("-p");
			options.add("--arg");
			options.add(propertyFilePath);
		} else {
			options.add("-p");
			options.add(propertyFilePath);
			
			if (verbose) {
				options.add("-v");
			}
		}
		
		return Arrays.copyOf(options.toArray(), options.size(), String[].class);
	}
	
	public SystemPropertyCenter(String[] args) throws URISyntaxException {

		CommandLineParser parser = new BasicParser();
		Options options = new Options();
		options.addOption("h", "help", false, "Display this help message.");
		options.addOption("v", "verbose", false, "Display debug information.");
		options.addOption("a", "application", true, "Application specified to run.");
		options.addOption("p", "property-file", true, "File path of the system property file.");
		options.addOption("b", "kafka-brokers", true, "Kafka brokers' ip addresses and ports.");
		options.addOption("z", "zookeeper", true, "Zookeeper server's ip address and port.");
		options.addOption("m", "spark-master", true, "Spark master (local[*], yarn, mesos).");
		options.addOption("dm", "spart-deploy-mode", true, "Spark deploy mode (cluster, client).");
		options.addOption("hdfs", "hdfs", true, "HDFS server ip address and port.");
		CommandLine commandLine;
		
		try {
			commandLine = parser.parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			System.out.println("Try using '-h' for more information.");
		    System.exit(0);
		    return;
		}

		if (commandLine.hasOption('h')) {
			//TODO Correct help message here.
			System.out.println("Help message under development...");
		    System.exit(0);
		    return;
		}
		if (commandLine.hasOption('v')) {
			System.out.println("Verbosity enabled!");
			verbose = true;
		}
		if (commandLine.hasOption("hdfs")) {
			hdfsNameNode = commandLine.getOptionValue("hdfs");
			if (verbose) {
				System.out.println("HDFS server: " + hdfsNameNode);
			}
		}
		if (commandLine.hasOption('a')) {
			applicationName = commandLine.getOptionValue('a');
			if (verbose) {
				System.out.println("To start application " + applicationName + "...");
			}
		}
		if (commandLine.hasOption('m')) {
			sparkMaster = commandLine.getOptionValue('m');
			//In case user specify deploy mode using "yarn-client" or "yarn-cluster".
			if (sparkMaster.contains("client")) {
				sparkDeployMode = "client";
			} else if (sparkMaster.contains("cluster")) {
				sparkDeployMode = "cluster";
			}
			
			if (sparkMaster.contains("yarn") && !onYARN) {
				onYARN = true;
				if (verbose) {
					System.out.println("To run on YARN...");
				}
			}
		}
		if (commandLine.hasOption('p')) {
			propertyFilePath = commandLine.getOptionValue('p');
			if (!propertyFilePath.equals("")) {
				//Load the property file.
				Properties systemProperties = new Properties();
				BufferedInputStream propInputStream;
				try {
					if (onYARN || propertyFilePath.contains("file:/") || propertyFilePath.contains("hdfs:/")) {
						if (verbose) {
							System.out.println("Loading properties using HDFS platform from " + propertyFilePath + "...");
						}
						
						FileSystem fileSystem = FileSystem.get(new URI(propertyFilePath), new Configuration());
						FSDataInputStream hdfsInputStream = fileSystem.open(new Path(propertyFilePath)); 
						propInputStream = new BufferedInputStream(hdfsInputStream);
					} else {
						if (verbose) {
							System.out.println("Loading properties locally from " + propertyFilePath + "...");
						}
						
						propInputStream = new BufferedInputStream(new FileInputStream(propertyFilePath));
					}
					systemProperties.load(propInputStream);
				} catch (IOException e) {
					System.err.printf("Cannot load system property file at specified path: \"%s\"!\n", propertyFilePath);
					System.out.println("Try use '-h' for more information.");
				    System.exit(0);
				    return;
				}
				
				//Digest the settings.
				for (Entry<Object, Object> entry : systemProperties.entrySet()) {
					if (verbose) {
						System.out.println("Read from property file: " + entry.getKey() + "=" + entry.getValue());
					}
					switch ((String) entry.getKey()) {
					case "zookeeper":
						zookeeper = (String) entry.getValue(); 
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
					case "checkpoint.directory":
						checkpointDir = (String) entry.getValue(); 
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
					case "hdfs.namenode":
						hdfsNameNode = (String) entry.getValue();
						break;
					}
				}
			}
		}
		if (commandLine.hasOption('b')) {
			kafkaBrokers = commandLine.getOptionValue('b');
		}
		if (commandLine.hasOption('z')) {
			zookeeper = commandLine.getOptionValue('z');
		}
		if (commandLine.hasOption("dm")) {
			sparkDeployMode = commandLine.getOptionValue("dm");
		}
		
		if (sparkMaster.contains("yarn") && !onYARN) {
			onYARN = true;
			if (verbose) {
				System.out.println("To run on YARN...");
			}
		}
	}
}
