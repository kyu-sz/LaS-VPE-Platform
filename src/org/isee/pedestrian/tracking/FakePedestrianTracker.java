package org.isee.pedestrian.tracking;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.isee.pedestrian.tracking.PedestrianTracker.Track.BoundingBox;

public class FakePedestrianTracker implements PedestrianTracker {
	
	private static final long serialVersionUID = 6140599886992421008L;
	private Random random = new Random();
	
	private Track generateRandomTrack() {
		Track track = new Track();
		track.startFrameIndex = random.nextInt();

		track.locationSequence = new LinkedList<>();
		int appearSpan = random.nextInt(31) + 1;
		for (int i = 0; i < appearSpan; ++i) {
			BoundingBox bbox = track.new BoundingBox();
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
