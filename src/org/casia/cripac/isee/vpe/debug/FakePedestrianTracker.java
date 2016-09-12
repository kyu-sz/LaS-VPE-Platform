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

package org.casia.cripac.isee.vpe.debug;

import java.util.Random;

import org.casia.cripac.isee.pedestrian.tracking.PedestrianTracker;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.pedestrian.tracking.Track.BoundingBox;

public class FakePedestrianTracker extends PedestrianTracker {

	private Random random = new Random();

	private Track generateRandomTrack(char[] videoURL) {
		Track track = new Track();
		track.startFrameIndex = random.nextInt(10000) + 1;
		track.id.videoURL = videoURL;

		int appearSpan = random.nextInt(31) + 1;
		track.locationSequence = new BoundingBox[appearSpan];
		for (int i = 0; i < appearSpan; ++i) {
			BoundingBox bbox = new BoundingBox();
			bbox.width = random.nextInt(640) + 1;
			bbox.height = random.nextInt(640) + 1;
			bbox.x = random.nextInt(bbox.width) + 1;
			bbox.y = random.nextInt(bbox.height) + 1;
			bbox.patchData = new byte[bbox.x * bbox.y * 3];

			track.locationSequence[i] = bbox;
		}

		return track;
	}

	private Track[] generateRandomTrackSet(char[] videoURL) {

		int numTracks = random.nextInt(30) + 3;
		Track[] tracks = new Track[numTracks];
		for (int i = 0; i < numTracks; ++i) {
			Track track = generateRandomTrack(videoURL);
			track.id.serialNumber = i;
			track.numTracks = numTracks;
			tracks[i] = track;
		}

		return tracks;
	}

	@Override
	public Track[] track(String videoURL) {
		return generateRandomTrackSet(videoURL.toCharArray());
	}

}
