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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

import org.casia.cripac.isee.pedestrian.tracking.PedestrianTracker;
import org.casia.cripac.isee.pedestrian.tracking.Track;
import org.casia.cripac.isee.pedestrian.tracking.Track.BoundingBox;

public class FakePedestrianTracker extends PedestrianTracker {
	
	private Random random = new Random();
	
	public Track generateRandomTrack(String videoURL) {
		Track track = new Track();
		track.startFrameIndex = random.nextInt(10000) + 1;
		track.videoURL = videoURL;

		track.locationSequence = new LinkedList<>();
		int appearSpan = random.nextInt(31) + 1;
		for (int i = 0; i < appearSpan; ++i) {
			BoundingBox bbox = track.new BoundingBox();
			bbox.width = random.nextInt(640) + 1;
			bbox.height = random.nextInt(640) + 1;
			bbox.x = random.nextInt(bbox.width) + 1;
			bbox.y = random.nextInt(bbox.height) + 1;
			bbox.patchData = new byte[bbox.x * bbox.y * 3];
			
			track.locationSequence.add(bbox);
		}
		
		return track;
	}
	
	public Set<Track> generateRandomTrackSet(String videoURL) {
		HashSet<Track> trackSet = new HashSet<>();
		
		int numTracks = random.nextInt(31) + 1;
		for (int i = 0; i < numTracks; ++i) {
			Track track = generateRandomTrack(videoURL);
			track.id = i;
			track.numTracks = numTracks;
			trackSet.add(track);
		}
		
		return trackSet;
	}

	@Override
	public Set<Track> track(String videoURL) {
		return generateRandomTrackSet(videoURL);
	}

}
