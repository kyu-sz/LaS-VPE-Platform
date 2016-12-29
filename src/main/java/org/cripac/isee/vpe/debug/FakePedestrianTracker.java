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

package org.cripac.isee.vpe.debug;

import org.cripac.isee.pedestrian.tracking.Tracker;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.pedestrian.tracking.Tracklet.BoundingBox;

import javax.annotation.Nonnull;
import java.util.Random;

public class FakePedestrianTracker extends Tracker {

    private Random random = new Random();

    private Tracklet generateRandomTracklet() {
        Tracklet tracklet = new Tracklet();
        tracklet.startFrameIndex = random.nextInt(10000) + 1;
        tracklet.id.videoID = "fake video";
        tracklet.id.serialNumber = -1;

        int appearSpan = random.nextInt(31) + 1;
        tracklet.locationSequence = new BoundingBox[appearSpan];
        for (int i = 0; i < appearSpan; ++i) {
            BoundingBox bbox = new BoundingBox();
            bbox.width = random.nextInt(640) + 1;
            bbox.height = random.nextInt(640) + 1;
            bbox.x = random.nextInt(bbox.width) + 1;
            bbox.y = random.nextInt(bbox.height) + 1;
            bbox.patchData = new byte[bbox.width * bbox.height * 3];
            random.nextBytes(bbox.patchData);

            tracklet.locationSequence[i] = bbox;
        }

        return tracklet;
    }

    private Tracklet[] generateRandomTrackSet() {
        int numTracks = random.nextInt(30) + 3;
        Tracklet[] tracklets = new Tracklet[numTracks];
        for (int i = 0; i < numTracks; ++i) {
            Tracklet tracklet = generateRandomTracklet();
            tracklet.id.serialNumber = i;
            tracklet.numTracklets = numTracks;
            tracklets[i] = tracklet;
        }

        return tracklets;
    }

    /**
     * Read a video from a URL, and perform pedestrian tracking on it.
     *
     * @param videoBytes Bytes of the video to conduct tracking on.
     * @return A set of tracklets of pedestrians.
     */
    @Override
    public Tracklet[] track(@Nonnull byte[] videoBytes) {
        return generateRandomTrackSet();
    }
}
