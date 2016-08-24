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

import org.casia.cripac.isee.vpe.common.ObjectFactory;

/**
 * Object factory of SynthesizedLogger.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class SynthesizedLoggerFactory extends ObjectFactory<SynthesizedLogger> {

	private static final long serialVersionUID = -6857919300875993611L;

	/**
	 * The address the logger may need to send logs to.
	 */
	String addr;

	/**
	 * The port the logger may need to send logs to.
	 */
	int port;

	/**
	 * Constructor of SynthesizedLoggerFactory with fixed log listener settings.
	 * 
	 * @param logListenerAddr
	 *            The address the logger may need to send logs to.
	 * @param logListenerPort
	 *            The port the logger may need to send logs to.
	 */
	public SynthesizedLoggerFactory(String logListenerAddr, int logListenerPort) {
		addr = logListenerAddr;
		port = logListenerPort;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.casia.cripac.isee.vpe.common.ObjectFactory#getObject()
	 */
	@Override
	public SynthesizedLogger getObject() {
		return new SynthesizedLogger(addr, port);
	}

}
