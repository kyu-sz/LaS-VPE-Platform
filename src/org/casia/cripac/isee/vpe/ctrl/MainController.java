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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.spark.launcher.SparkLauncher;
import org.casia.cripac.isee.vpe.alg.PedestrianAttrRecogApp;
import org.casia.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter.NoAppSpecifiedException;
import org.casia.cripac.isee.vpe.debug.CommandGeneratingApp;
import org.xml.sax.SAXException;

/**
 * The MainController class initializes the system environment (currently only
 * checks the required topics), and provides entrance to start any spark
 * streaming applications.
 * 
 * @author Ken Yu, CRIPAC, 2016
 * 
 */
public class MainController {

	public static boolean listening = true;

	public static void main(String[] args) throws NoAppSpecifiedException, URISyntaxException, IOException,
			ParserConfigurationException, SAXException {

		// Analyze the command line and store the options into a system property
		// center.
		SystemPropertyCenter propertyCenter = new SystemPropertyCenter(args);

		// Prepare system configuration.
		if (propertyCenter.onYARN) {
			System.setProperty("SPARK_YARN_MODE", "true");

			DatagramSocket server = new DatagramSocket(0);
			server.setSoTimeout(1000);

			// Create a thread to listen to messages.
			Thread listener = new Thread(new Runnable() {

				@Override
				public void run() {
					byte[] recvBuf = new byte[1000];
					DatagramPacket recvPacket = new DatagramPacket(recvBuf, recvBuf.length);
					while (listening) {
						try {
							server.receive(recvPacket);
							String recvStr = new String(recvPacket.getData(), 0, recvPacket.getLength());
							System.out.println(recvStr);
						} catch (SocketTimeoutException e) {

						} catch (IOException e) {
							e.printStackTrace();
							break;
						}
					}
					System.out.println("|INFO|Stop listening!");
					server.close();
				}
			});
			listener.start();

			propertyCenter.messageListenerAddress = InetAddress.getLocalHost().getHostName();
			propertyCenter.messageListenerPort = server.getLocalPort();

			String[] arguments = propertyCenter.getArgs();

			if (propertyCenter.verbose) {
				System.out.print("|INFO|Submitting with args:");
				for (String arg : arguments) {
					System.out.print(" " + arg);
				}
				System.out.println("");
			}

			Process sparkLauncherProcess = new SparkLauncher().setAppResource(propertyCenter.jarPath)
					.setMainClass(AppManager.getMainClassName(propertyCenter.appName))
					.setMaster(propertyCenter.sparkMaster)
					.setAppName(propertyCenter.appName).setPropertiesFile(propertyCenter.sparkConfFilePath)
					.setVerbose(propertyCenter.verbose).addFile(propertyCenter.log4jPropertiesFilePath)
					.addFile(propertyCenter.systemPropertiesFilePath)
					.setConf(SparkLauncher.DRIVER_MEMORY, propertyCenter.driverMem)
					.setConf(SparkLauncher.EXECUTOR_CORES, "" + propertyCenter.executorCores)
					.setConf(SparkLauncher.EXECUTOR_MEMORY, propertyCenter.executorMem)
					.setConf(SparkLauncher.DRIVER_MEMORY, propertyCenter.driverMem).addAppArgs(propertyCenter.getArgs())
					.launch();

			InputStreamReaderRunnable infoStreamReaderRunnable = new InputStreamReaderRunnable(
					sparkLauncherProcess.getInputStream(), "INFO");
			Thread infoThread = new Thread(infoStreamReaderRunnable, "LogStreamReader info");
			infoThread.start();

			InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(
					sparkLauncherProcess.getErrorStream(), "ERROR");
			Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
			errorThread.start();

			try {
				System.out.println("|INFO|Waiting for finish...");
				int ret = sparkLauncherProcess.waitFor();
				System.out.println("|INFO|Finished! Exit code: " + ret);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			listening = false;
		} else {
			String[] arguments = propertyCenter.getArgs();

			// Run locally.
			switch (propertyCenter.appName) {
			case MessageHandlingApp.APP_NAME:
				MessageHandlingApp.main(arguments);
				break;
			case MetadataSavingApp.APP_NAME:
				MetadataSavingApp.main(arguments);
				break;
			case PedestrianAttrRecogApp.APP_NAME:
				PedestrianAttrRecogApp.main(arguments);
				break;
			case PedestrianTrackingApp.APP_NAME:
				PedestrianTrackingApp.main(arguments);
				break;
			case CommandGeneratingApp.APP_NAME:
				CommandGeneratingApp.main(arguments);
				break;
			default:
				System.err.printf("No application named \"%s\"!\n", propertyCenter.appName);
			case "":
				System.out.println("Try using '-h' for more information.");
				throw new NoAppSpecifiedException();
			}
		}
	}
}
