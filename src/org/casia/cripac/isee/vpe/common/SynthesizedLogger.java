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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * The SynthesizedLogger class synthesizes various logging methods,
 * like log4j, raw console, socket...
 * It welcomes modification by developers with their own demands.  
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class SynthesizedLogger {
	Queue<String> messageQueue = new LinkedList<>();
	Logger log4jLogger;
	DatagramSocket sender;
	InetAddress address;
	int port;
	Thread senderThread;
	
	/**
	 * Currently only creates a log4j logger.
	 */
	public SynthesizedLogger(String messageListenerAddr, int messageListenerPort) {
		PropertyConfigurator.configure("log4j.properties");
		log4jLogger = LogManager.getRootLogger();
		log4jLogger.setLevel(Level.INFO);
		
		try {
			address = InetAddress.getByName(messageListenerAddr);
			sender = new DatagramSocket();
			port = messageListenerPort;
			senderThread = new Thread(new Runnable() {
				
				@Override
				public void run() {
					while (true) {
						while (messageQueue.isEmpty()) {
							try {
								Thread.sleep(100);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
						String message = messageQueue.poll();
						byte[] sendBuf = message.getBytes();
						DatagramPacket sendPacket = new DatagramPacket(sendBuf, sendBuf.length, address, port);
						try {
							sender.send(sendPacket);
						} catch (IOException e) {
							e.printStackTrace();
							break;
						}
					}
					sender.close();
				}
			});
			senderThread.start();
		} catch (SocketException | UnknownHostException e1) {
			e1.printStackTrace();
		}
	}
	
	public void debug(Object message) {
		log4jLogger.debug(message);
		System.out.println("DEBUG: " + message);
		messageQueue.add("DEBUG: " + message);
	}
	public void debug(Object message, Throwable t) {
		log4jLogger.debug(message, t);
		System.out.println("DEBUG: " + message);
		t.printStackTrace();
		messageQueue.add("DEBUG: " + message);
		String stackTraceMsg = "";
		StackTraceElement[] stackTrace = t.getStackTrace();
		for (StackTraceElement element : stackTrace) {
			stackTraceMsg = stackTraceMsg + element.toString() + "\n";
		}
		messageQueue.add(stackTraceMsg);
	}
	
	public void info(Object message) {
		log4jLogger.info(message);
		System.out.println("INFO: " + message);
		messageQueue.add("INFO: " + message);
	}
	public void info(Object message, Throwable t) {
		log4jLogger.info(message, t);
		System.out.println("INFO: " + message);
		t.printStackTrace();
		messageQueue.add("INFO: " + message);
		String stackTraceMsg = "";
		StackTraceElement[] stackTrace = t.getStackTrace();
		for (StackTraceElement element : stackTrace) {
			stackTraceMsg = stackTraceMsg + element.toString() + "\n";
		}
		messageQueue.add(stackTraceMsg);
	}
	
	public void error(Object message) {
		log4jLogger.error(message);
		System.err.println("ERROR: " + message);
		messageQueue.add("ERROR: " + message);
	}
	public void error(Object message, Throwable t) {
		log4jLogger.error(message, t);
		System.err.println("ERROR: " + message);
		t.printStackTrace();
		messageQueue.add("ERROR: " + message);
		String stackTraceMsg = "";
		StackTraceElement[] stackTrace = t.getStackTrace();
		for (StackTraceElement element : stackTrace) {
			stackTraceMsg = stackTraceMsg + element.toString() + "\n";
		}
		messageQueue.add(stackTraceMsg);
	}
	
	public void fatal(Object message) {
		log4jLogger.fatal(message);
		System.err.println("FATAL: " + message);
		messageQueue.add("FATAL: " + message);
	}
	public void fatal(Object message, Throwable t) {
		log4jLogger.fatal(message, t);
		System.err.println("FATAL: " + message);
		t.printStackTrace();
		messageQueue.add("FATAL: " + message);
		String stackTraceMsg = "";
		StackTraceElement[] stackTrace = t.getStackTrace();
		for (StackTraceElement element : stackTrace) {
			stackTraceMsg = stackTraceMsg + element.toString() + "\n";
		}
		messageQueue.add(stackTraceMsg);
	}
}
