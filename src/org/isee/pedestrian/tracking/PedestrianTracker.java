package org.isee.pedestrian.tracking;

import java.io.Serializable;
import java.net.URL;
import java.util.List;
import java.util.Set;

public interface PedestrianTracker extends Serializable {
	
	public class Track {
		
		public class BoundingBox {
			int x;
			int y;
			int width;
			int height;
		}
		
		public int startFrameIndex;
		List<BoundingBox> locationSequence;
	}
	
	public Set<Track> track(String videoURL);
	public Set<Track> track(List<String> frameURLs);
}
