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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.spark.launcher.SparkLauncher;
import org.apache.zookeeper.KeeperException.UnimplementedException;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter;
import org.casia.cripac.isee.vpe.common.SystemPropertyCenter.NoAppSpecifiedException;
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
			ParserConfigurationException, SAXException, UnimplementedException {

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

			final class ProcessWithName {
				public Process process;
				public String name;

				public ProcessWithName(Process process, String name) {
					this.process = process;
					this.name = name;
				}
			}
			List<ProcessWithName> processesWithNames = new LinkedList<>();
			for (String appName : propertyCenter.appsToStart) {
				Process sparkLauncherProcess = new SparkLauncher().setAppResource(propertyCenter.jarPath)
						.setMainClass(AppManager.getMainClassName(appName)).setMaster(propertyCenter.sparkMaster)
						.setAppName(appName).setPropertiesFile(propertyCenter.sparkConfFilePath)
						.setVerbose(propertyCenter.verbose).addFile(propertyCenter.log4jPropertiesFilePath)
						.addFile(propertyCenter.systemPropertiesFilePath)
						.setConf(SparkLauncher.DRIVER_MEMORY, propertyCenter.driverMem)
						.setConf(SparkLauncher.EXECUTOR_CORES, "" + propertyCenter.executorCores)
						.setConf(SparkLauncher.EXECUTOR_MEMORY, propertyCenter.executorMem)
						.setConf(SparkLauncher.DRIVER_MEMORY, propertyCenter.driverMem)
						.addAppArgs(propertyCenter.getArgs()).launch();
				processesWithNames.add(new ProcessWithName(sparkLauncherProcess, appName));

				InputStreamReaderRunnable infoStreamReaderRunnable = new InputStreamReaderRunnable(
						sparkLauncherProcess.getInputStream(), "INFO");
				Thread infoThread = new Thread(infoStreamReaderRunnable, "LogStreamReader info");
				infoThread.start();

				InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(
						sparkLauncherProcess.getErrorStream(), "ERROR");
				Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
				errorThread.start();
			}

			for (ProcessWithName processWithName : processesWithNames) {
				System.out.println("|INFO|Waiting for process " + processWithName.name + " to finish...");
			}
			while (!processesWithNames.isEmpty()) {
				for (ProcessWithName processWithName : processesWithNames) {
					try {
						boolean exited = processWithName.process.waitFor(100, TimeUnit.MILLISECONDS);
						if (exited) {
							System.out.println("|INFO|Process " + processWithName.name + "finished! Exit code: "
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
