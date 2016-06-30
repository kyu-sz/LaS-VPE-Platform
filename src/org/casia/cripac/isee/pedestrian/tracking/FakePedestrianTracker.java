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

package org.casia.cripac.isee.pedestrian.tracking;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class FakePedestrianTracker implements PedestrianTracker {
	
	private Random random = new Random();
	
	private Track generateRandomTrack() {
		Track track = new Track();
		track.startFrameIndex = random.nextInt(10000) + 1;

		track.locationSequence = new LinkedList<>();
		int appearSpan = random.nextInt(31) + 1;
		for (int i = 0; i < appearSpan; ++i) {
			org.casia.cripac.isee.pedestrian.tracking.Track.BoundingBox bbox = track.new BoundingBox();
			bbox.x = random.nextInt();
			bbox.y = random.nextInt();
			bbox.width = random.nextInt();
			bbox.height = random.nextInt();
			
			track.locationSequence.add(bbox);
		}
		
		return track;
	}
	
	private Set<Track> generateRandomTrackSet() {
		HashSet<Track> trackSet = new HashSet<>();
		
		int trackNum = random.nextInt(31) + 1;
		for (int i = 0; i < trackNum; ++i) {
			trackSet.add(generateRandomTrack());
		}
		
		return trackSet;
	}

	@Override
	public Set<Track> track(String videoURL) {
		return generateRandomTrackSet();
	}

	@Override
	public Set<Track> track(List<String> frameURLs) {
		return generateRandomTrackSet();
	}

}
