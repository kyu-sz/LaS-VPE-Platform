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

import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.util.Factory;

import javax.annotation.Nonnull;

/**
 * Object factory of SynthesizedLogger.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class SynthesizedLoggerFactory implements Factory<Logger> {

    private static final long serialVersionUID = 784961952714587116L;
    private String username;
    private SystemPropertyCenter propCenter;

    /**
     * Constructor of SynthesizedLoggerFactory with fixed log listener settings.
     *
     * @param username    Name of the logger user.
     * @param propCenter Properties of the system.
     */
    public SynthesizedLoggerFactory(@Nonnull String username,
                                    @Nonnull SystemPropertyCenter propCenter) {
        this.username = username;
        this.propCenter = propCenter;
    }

    @Nonnull
    @Override
    public SynthesizedLogger produce() {
        return new SynthesizedLogger(username, propCenter);
    }
}
