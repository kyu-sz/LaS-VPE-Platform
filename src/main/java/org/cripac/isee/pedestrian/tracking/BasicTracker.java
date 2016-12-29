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

import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.tracking.VideoDecoder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The BasicTracker class is a JNI class of a pedestrian tracking algorithm used
 * within the Center for Research on Intelligent Perception and Computing(CRIPAC),
 * Institute of Automation, Chinese Academy of Science.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class BasicTracker extends Tracker {

    private static int instanceCnt = 0;
    private Lock instCntLock = new ReentrantLock();

    static {
        System.out.println("Loading native libraries for BasicTracker from "
                + System.getProperty("java.library.path"));
        System.loadLibrary("basic_pedestrian_tracker_jni");
        System.out.println("Native libraries for BasicTracker successfully loaded!");
    }

    private byte[] conf;
    private Logger logger;

    public BasicTracker(@Nonnull byte[] conf) {
        this(conf, null);
    }

    /**
     * Construct a tracker with a configuration. The configuration should be
     * provided in a form of byte array.
     *
     * @param conf The byte data of the configuration file.
     */
    public BasicTracker(@Nonnull byte[] conf,
                        @Nullable Logger logger) {
        this.conf = conf;
        if (logger == null) {
            this.logger = logger;
        } else {
            this.logger = new ConsoleLogger();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see Tracker#track(java.lang.String)
     */
    @Override
    public Tracklet[] track(@Nonnull byte[] videoBytes) {
        // Limit instances on a single node.
        while (true) {
            instCntLock.lock();
            if (instanceCnt < 5) {
                ++instanceCnt;
                try {
                    logger.info("Tracker instance count: " + instanceCnt);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            }
            instCntLock.unlock();
            logger.debug("Current tracker instance number is " + instanceCnt
                    + ". Waiting for previous tasks to finish...");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        VideoDecoder videoDecoder = new VideoDecoder(videoBytes);
        logger.debug("Initialized video decoder!");
        VideoDecoder.VideoInfo videoInfo = videoDecoder.getVideoInfo();
        logger.debug("To perform tracking on video with width=" + videoInfo.width + " height=" + videoInfo.height + "!");

        if (conf == null) {
            logger.fatal("Configuration file is NULL!");
            return null;
        }
        long trackerPointer = initialize(videoInfo.width, videoInfo.height, videoInfo.channels, conf);
        logger.debug("Initialized tracker!");

        int cnt = 0;
        // Every time a frame is retrieved during decoding, it is immediately fed into the tracker,
        // so as to save runtime memory.
        while (true) {
            byte[] frame = videoDecoder.nextFrame();
            if (frame == null) {
                break;
            }
            int ret = feedFrame(trackerPointer, frame);
            if (ret != 0) {
                break;
            }
            ++cnt;
            if (cnt % 1000 == 0) {
                logger.debug("Tracked " + cnt + " frames!");
            }
        }

        logger.debug("Totally processed " + cnt + " framed!");
        logger.debug("Getting targets...");
        Tracklet[] targets = getTargets(trackerPointer);
        logger.debug("Got " + targets.length + " targets!");
        free(trackerPointer);

        instCntLock.lock();
        --instanceCnt;
        try {
            logger.info("Tracker instance count: " + instanceCnt);
        } catch (Exception e) {
            e.printStackTrace();
        }
        instCntLock.unlock();

        return targets;
//        return new FakePedestrianTracker().track(videoBytes);
    }

    /**
     * Initialize a native tracker.
     *
     * @param width    The width of frames of the video to process.
     * @param height   The height of frames of the video to process.
     * @param channels The channels of frames of the video to process.
     * @param conf     Bytes of a configuration the tracker uses.
     * @return The pointer of the initialized tracker.
     */
    private native long initialize(int width,
                                   int height,
                                   int channels,
                                   @Nonnull byte[] conf);

    /**
     * Feed a frame into the tracker. The tracker is expected to process the video frame by frame.
     *
     * @param p     The pointer of an initialized tracker.
     * @param frame BGR bytes of a decoded frame.
     * @return 0 on success and -1 on failure.
     */
    private native int feedFrame(long p,
                                 @Nonnull byte[] frame);

    /**
     * Get tracked targets in currently input frames.
     *
     * @param p The pointer of an initialized tracker the user has fed frames to.
     * @return An array of tracklets, each representing a target.
     */
    private native Tracklet[] getTargets(long p);

    private native void free(long p);
}
