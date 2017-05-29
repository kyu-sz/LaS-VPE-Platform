/*
 * This file is part of las-vpe-platform.
 *
 * las-vpe-platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * las-vpe-platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with las-vpe-platform. If not, see <http://www.gnu.org/licenses/>.
 *
 * Created by ken.yu on 17-3-13.
 */
package org.cripac.isee.vpe.ctrl;

import com.sun.management.OperatingSystemMXBean;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;
import java.lang.management.ManagementFactory;

public class MonitorThread extends Thread {

    private final Logger logger;
    private final Runtime runtime = Runtime.getRuntime();
    private final OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);

    public MonitorThread(Logger logger)
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException {
        this.logger = logger;
        logger.info("Running with " + osBean.getAvailableProcessors() + " " + osBean.getArch() + " processors");
    }

    @Override
    public void run() {
        logger.debug("Starting monitoring!");
        //noinspection InfiniteLoopStatement
        while (true) {
            logger.info("Memory consumption: "
                    + ((runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)) + "/"
                    + (runtime.maxMemory() / (1024 * 1024)) + "/"
                    + runtime.totalMemory() + "/"
                    + osBean.getTotalPhysicalMemorySize() + "/");

            logger.info("CPU load: " + osBean.getProcessCpuLoad()
                    + "/" + osBean.getSystemCpuLoad());

            try {
                sleep(10000);
            } catch (InterruptedException ignored) {
            }
        }
    }
}
