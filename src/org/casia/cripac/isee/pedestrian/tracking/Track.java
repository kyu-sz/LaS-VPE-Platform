package org.casia.cripac.isee.pedestrian.tracking;

import java.io.Serializable;
import java.util.List;

public class Track implements Serializable {
	
	public class BoundingBox implements Serializable {
		private static final long serialVersionUID = -5261437055893590056L;
		
		public int x;
		public int y;
		public int width;
		public int height;
	}
	
	private static final long serialVersionUID = -6133927313545472821L;
	
	public int startFrameIndex;
	public List<BoundingBox> locationSequence;
}