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

package org.casia.cripac.isee.vpe.ctrl;
/**
 * 
 */

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.URISyntaxException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.casia.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.casia.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.casia.cripac.isee.vpe.common.HadoopUtils;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter.NoAppSpecifiedException;
import org.casia.cripac.isee.vpe.debug.CommandGeneratingApp;
import org.xml.sax.SAXException;

/**
 * The MainController class initializes the system environment (currently only checks the required topics),
 * and provides entrance to start any spark streaming applications.
 * 
 * @author Ken Yu, CRIPAC, 2016
 * 
 */
public class MainController {
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws NoAppSpecifiedException, URISyntaxException, IOException, ParserConfigurationException, SAXException {
		
		//Analyze the command line and store the options into a system property center.
		SystemPropertyCenter propertyCenter = new SystemPropertyCenter(args);
		
		//Check whether topics required have been created in the Kafka servers.
		if (propertyCenter.verbose) {
			System.out.println("Checking required topics...");
		}
		TopicManager.checkTopics(propertyCenter);

		SparkConf sparkConf = new SparkConf();
		//Prepare system configuration.
		if (propertyCenter.onYARN) {
			System.setProperty("SPARK_YARN_MODE", "true");
			sparkConf.set("mapreduce.framework.name", "yarn");
			sparkConf.set("yarn.resourcemanager.hostname", propertyCenter.yarnResourceManagerHostname);
			
			//Load Hadoop configuration from XML files.
			Configuration hadoopConf = HadoopUtils.getDefaultConf();
			
//			arguments = new String[] {
//				"--name", "SparkPiFromJava",
//				"--class", "org.apache.spark.examples.SparkPi",
//				"--jar", "/home/ken/spark-1.6.1-bin-hadoop2.6/lib/spark-examples-1.6.1-hadoop2.6.0.jar",
//				"--arg", "10"
//			};
			
			DatagramSocket server = new DatagramSocket(0);
			
			//Create a thread to listen to messages.
			Thread listener = new Thread(new Runnable() {
				
				@Override
				public void run() {
					byte[] recvBuf = new byte[1000];
					DatagramPacket recvPacket = new DatagramPacket(recvBuf, recvBuf.length);
					try {
						while (true) {
							server.receive(recvPacket);
							String recvStr = new String(recvPacket.getData(), 0, recvPacket.getLength());
							System.out.println(recvStr);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					server.close();
				}
			});
			listener.start();
			
			propertyCenter.messageListenerAddress = InetAddress.getLocalHost().getHostName();
			propertyCenter.messageListenerPort = server.getLocalPort();
			
			String[] arguments = propertyCenter.generateCommandLineOpts();
			
			if (propertyCenter.verbose) {
				System.out.print("Submitting with args:");
				for (String arg : arguments) {
					System.out.print(" " + arg);
				}
				System.out.println("");
			}
			
			//Submit to Spark.
			ClientArguments yarnClientArguments = new ClientArguments(arguments, sparkConf);
			new Client(yarnClientArguments, hadoopConf, sparkConf).run();
			
			listener.stop();
		} else {
			String[] arguments = propertyCenter.generateCommandLineOpts();
			
			//Run locally.
			switch (propertyCenter.applicationName) {
			case MessageHandlingApp.APPLICATION_NAME:
				MessageHandlingApp.main(arguments);
				break;
			case MetadataSavingApp.APPLICATION_NAME:
				MetadataSavingApp.main(arguments);
				break;
			case PedestrianAttrRecogApp.APPLICATION_NAME:
				PedestrianAttrRecogApp.main(arguments);
				break;
			case PedestrianTrackingApp.APPLICATION_NAME:
				PedestrianTrackingApp.main(arguments);
				break;
			case CommandGeneratingApp.APPLICATION_NAME:
				CommandGeneratingApp.main(arguments);
				break;
			default:
				System.err.printf("No application named \"%s\"!\n", propertyCenter.applicationName);
			case "":
				System.out.println("Try using '-h' for more information.");
				throw new NoAppSpecifiedException();
			}
		}
	}
}
