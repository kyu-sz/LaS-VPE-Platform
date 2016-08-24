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
package org.casia.cripac.isee.vpe.util.logging;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * The SynthesizedLogger class synthesizes various logging methods, like log4j,
 * raw console, socket... It welcomes modification by developers with their own
 * demands.
 * 
 * @author Ken Yu, CRIPAC, 2016
 */
public class SynthesizedLogger {

	private Logger log4jLogger;
	private DatagramSocket sender;
	private InetAddress listenerAddr;
	private String name;
	private int listenerPort;

	/**
	 * Currently only creates a log4j logger.
	 * 
	 * @throws UnknownHostException
	 * @throws SocketException
	 */
	public SynthesizedLogger(String messageListenerAddr, int messageListenerPort) {
		PropertyConfigurator.configure("log4j.properties");
		log4jLogger = LogManager.getRootLogger();
		log4jLogger.setLevel(Level.INFO);

		try {
			name = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			name = "Unknown Host";
		}

		try {
			sender = new DatagramSocket();
		} catch (SocketException e) {
			e.printStackTrace();
			sender = null;
		}

		try {
			listenerAddr = InetAddress.getByName(messageListenerAddr);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			listenerAddr = null;
			sender = null;
		}

		listenerPort = messageListenerPort;
	}

	private void send(String message) {
		if (sender != null) {
			byte[] sendBuf = message.getBytes();
			DatagramPacket sendPacket = new DatagramPacket(sendBuf, sendBuf.length, listenerAddr, listenerPort);
			System.out.println("|INFO|" + name + ":\tReporting to " + listenerAddr + ":" + listenerPort);
			try {
				sender.send(sendPacket);
			} catch (IOException e) {
				System.out.println(
						"|ERROR|" + name + ":\tError occurred when reporting to " + listenerAddr + ":" + listenerPort);
				e.printStackTrace();
				sender = null;
			}
		} else {
			System.out.println("|ERROR|" + name + ":\tSender dead!");
		}
	}

	public void debug(Object message) {
		log4jLogger.debug(message);
		String richMsg = "|DEBUG|" + name + ":\t" + message;
		System.out.println(richMsg);
		send((String) richMsg);
	}

	public void debug(Object message, Throwable t) {
		log4jLogger.debug(message, t);
		String richMsg = "|DEBUG|" + name + ":\t" + message;
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

	public void info(Object message) {
		log4jLogger.info(message);
		String richMsg = "|INFO|" + name + ":\t" + message;
		System.out.println(richMsg);
		send(richMsg);
	}

	public void info(Object message, Throwable t) {
		log4jLogger.info(message, t);
		String richMsg = "|INFO|" + name + ":\t" + message;
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

	public void error(Object message) {
		log4jLogger.error(message);
		String richMsg = "|ERROR|" + name + ":\t" + message;
		System.err.println(richMsg);
		send(richMsg);
	}

	public void error(Object message, Throwable t) {
		log4jLogger.error(message, t);
		String richMsg = "|ERROR|" + name + ":\t" + message;
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

	public void fatal(Object message) {
		log4jLogger.fatal(message);
		String richMsg = "|FATAL|" + name + ":\t" + message;
		System.err.println(richMsg);
		send(richMsg);
	}

	public void fatal(Object message, Throwable t) {
		log4jLogger.fatal(message, t);
		String richMsg = "|FATAL|" + name + ":\t" + message;
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
