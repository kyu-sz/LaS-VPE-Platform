/***********************************************************************
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
 ************************************************************************/

package org.cripac.isee.vpe.util.logging;

import org.apache.log4j.Level;
import org.cripac.isee.vpe.util.Factory;

/**
 * Object factory of SynthesizedLogger.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class SynthesizedLoggerFactory implements Factory<SynthesizedLogger> {

    private static final long serialVersionUID = -6857919300875993611L;

    /**
     * The address the logger may need to sendWithLog logs to.
     */
    private String addr;

    /**
     * The port the logger may need to sendWithLog logs to.
     */
    private int port;

    private String appName;
    private Level level;

    /**
     * Constructor of SynthesizedLoggerFactory with fixed log listener settings.
     *
     * @param appName         Name of the application using the logger.
     * @param level           Level of logging.
     * @param logListenerAddr The address the logger may need to sendWithLog logs to.
     * @param logListenerPort The port the logger may need to sendWithLog logs to.
     */
    public SynthesizedLoggerFactory(String appName,
                                    Level level,
                                    String logListenerAddr,
                                    int logListenerPort) {
        this.appName = appName;
        this.level = level;
        this.addr = logListenerAddr;
        this.port = logListenerPort;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.casia.cripac.isee.vpe.common.ObjectFactory#getObject()
     */
    @Override
    public SynthesizedLogger produce() {
        return new SynthesizedLogger(appName, level, addr, port);
    }

}
