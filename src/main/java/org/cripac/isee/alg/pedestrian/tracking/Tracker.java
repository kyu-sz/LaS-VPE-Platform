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

package org.cripac.isee.alg.pedestrian.tracking;

import org.bytedeco.javacv.FrameGrabber;

import javax.annotation.Nonnull;
import java.io.InputStream;

/**
 * The Tracker class is the base class of all pedestrian tracking
 * classes. Any subclass is required to implement a simple 'track' method, which
 * takes in a URL of a video and returns a set of track.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public interface Tracker {

    /**
     * Read a video from a URL, and perform pedestrian tracking on it.
     *
     * @param videoStream Video stream to conduct tracking on.
     * @return A set of tracklets of pedestrians.
     */
    @Nonnull
    Tracklet[] track(@Nonnull InputStream videoStream) throws FrameGrabber.Exception;
}
