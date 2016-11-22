/**
 * This file is part of LaS-VPE Platform.
 * <p>
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * Created by ken.yu on 16-10-5.
 * <p>
 * Created by ken.yu on 16-10-5.
 * <p>
 * Created by ken.yu on 16-10-5.
 */

/**
 * Created by ken.yu on 16-10-5.
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

    private byte[] conf;
    private Singleton<Logger> loggerSingleton;

    public BasicTrackerFactory(@Nonnull byte[] conf,
                               @Nullable Singleton<Logger> loggerSingleton) {
        this.conf = conf;
        if (loggerSingleton == null) {
            this.loggerSingleton = loggerSingleton;
        } else {
            try {
                this.loggerSingleton = new Singleton<>(() -> new ConsoleLogger());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Produce a new BasicTracker instance.
     *
     * @return A new BasicTracker instance newly produced.
     * @throws Exception On failure creating a new BasicTracker instance.
     */
    @Override
    public BasicTracker produce() throws Exception {
        return new BasicTracker(conf, loggerSingleton.getInst());
    }
}
