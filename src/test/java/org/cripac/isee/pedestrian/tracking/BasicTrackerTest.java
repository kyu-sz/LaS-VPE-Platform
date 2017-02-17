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

package org.cripac.isee.pedestrian.tracking;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Level;
import org.cripac.isee.vpe.alg.PedestrianTrackingApp;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.junit.Assert;

import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Created by ken.yu on 16-10-23.
 */
public class BasicTrackerTest {
    //    @Test
    public void initialize() throws Exception {
        System.out.println("Performing memory leak test...");

        byte[] conf = IOUtils.toByteArray(new FileInputStream(
                "conf/" + PedestrianTrackingApp.APP_NAME + "/isee-basic/CAM01_0.conf"));
        for (int i = 0; i < 100000; ++i) {
            BasicTracker tracker = new BasicTracker(conf, new ConsoleLogger(Level.DEBUG));
        }
    }

    //    @Test
    public void track() throws Exception {
        System.out.println("Performing validness test...");

        System.out.println("Reading video...");
        InputStream videoStream = new FileInputStream("src/test/resources/20131220184349-20131220184937.h264");

        System.out.println("Native library path: " + System.getProperty("java.library.path"));
        System.out.println("Creating tracker...");
        BasicTracker tracker = new BasicTracker(
                IOUtils.toByteArray(new FileInputStream(
                        "conf/" + PedestrianTrackingApp.APP_NAME + "/isee-basic/CAM01_0.conf")),
                new ConsoleLogger(Level.DEBUG));

        System.out.println("Start tracking...");
        Tracklet[] tracklets = tracker.track(videoStream);

        System.out.println("Tracked " + tracklets.length + " pedestrians!");
        for (Tracklet tracklet : tracklets) {
            System.out.println(tracklet);
        }

        Assert.assertEquals(2, tracklets.length);
        Assert.assertEquals(3495, tracklets[0].startFrameIndex);
        Assert.assertEquals(69, tracklets[0].locationSequence.length);
        Assert.assertEquals(3565, tracklets[1].startFrameIndex);
        Assert.assertEquals(4, tracklets[1].locationSequence.length);
    }
}