/*
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
 */

package org.cripac.isee.vpe.util.tracking;

import org.cripac.isee.pedestrian.tracking.BasicTracker;
import org.cripac.isee.vpe.util.Factory;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The BasicTrackerFactory class is a factory class for creating
 * BasicTracker instances.
 */
public class BasicTrackerFactory implements Factory<BasicTracker> {

    private static final long serialVersionUID = -3126490020471068124L;
    private byte[] conf;
    private Singleton<Logger> loggerSingleton;

    public BasicTrackerFactory(@Nonnull byte[] conf,
                               @Nullable Singleton<Logger> loggerSingleton) {
        this.conf = conf;
        if (loggerSingleton != null) {
            this.loggerSingleton = loggerSingleton;
        } else {
            try {
                this.loggerSingleton = new Singleton<>(ConsoleLogger::new);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Produce a new BasicTracker instance.
     *
     * @return A new BasicTracker instance newly produced.
     */
    @Nonnull
    @Override
    public BasicTracker produce() {
        Logger logger;
        try {
            logger = loggerSingleton.getInst();
        } catch (Exception e) {
            logger = new ConsoleLogger();
            logger.error("Cannot get logger.", e);
        }
        return new BasicTracker(conf, logger);
    }
}
