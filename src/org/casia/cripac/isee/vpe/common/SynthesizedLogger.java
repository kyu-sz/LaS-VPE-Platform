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
	Logger log4jLogger;
	
	/**
	 * Currently only creates a log4j logger.
	 */
	public SynthesizedLogger() {
		PropertyConfigurator.configure("log4j.properties");
		log4jLogger = LogManager.getRootLogger();
		log4jLogger.setLevel(Level.INFO);
	}
	
	public void info(Object message) {
		log4jLogger.info(message);
		System.out.println("INFO: " + message);
	}
	public void info(Object message, Throwable t) {
		log4jLogger.info(message, t);
		System.out.println("ERROR: " + message);
		t.printStackTrace();
	}
	
	public void error(Object message) {
		log4jLogger.error(message);
		System.out.println("ERROR: " + message);
	}
	public void error(Object message, Throwable t) {
		log4jLogger.error(message, t);
		System.out.println("ERROR: " + message);
		t.printStackTrace();
	}
	
	public void fatal(Object message) {
		log4jLogger.fatal(message);
		System.out.println("FATAL: " + message);
	}
	public void fatal(Object message, Throwable t) {
		log4jLogger.fatal(message, t);
		System.out.println("ERROR: " + message);
		t.printStackTrace();
	}
	
	public void debug(Object message) {
		log4jLogger.debug(message);
		System.out.println("DEBUG: " + message);
	}
	public void debug(Object message, Throwable t) {
		log4jLogger.debug(message, t);
		System.out.println("ERROR: " + message);
		t.printStackTrace();
	}
}
