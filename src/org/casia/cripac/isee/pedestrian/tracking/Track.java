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

import java.io.Serializable;
import java.util.List;

/**
 * The Track class stores a sequence of bounding boxes,
 * representing the track of a pedestrian in a video.
 * 
 * TODO Should assign an ID to each track or use some other methods to identify them.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class Track implements Serializable {
	
	/**
	 * The BoundingBox class stores the location of an object in a single static frame.
	 * @author Ken Yu, CRIPAC, 2016
	 *
	 */
	public class BoundingBox implements Serializable {
		
		private static final long serialVersionUID = -5261437055893590056L;
		
		/**
		 * x-coordinate of the point on the left-upper corner of the bounding box.
		 */
		public int x;
		
		/**
		 * y-coordinate of the point on the left-upper corner of the bounding box.
		 */
		public int y;
		/**
		 * The width of the bounding box.
		 */
		public int width;
		/**
		 * The height of the bounding box.
		 */
		public int height;
	}
	
	private static final long serialVersionUID = -6133927313545472821L;
	
	/**
	 * The ID of the track in the video.
	 * The ID is usually organized in a chronological order.
	 * Track generating algorithms, like pedestrian tracking algorithms, should be responsible of generating this ID.
	 * Its default value is -1, meaning the ID has not been generated yet.
	 */
	public int trackID = -1;
	/**
	 * The URL indicating where the source video is stored.
	 */
	public String videoURL = null;
	/**
	 * The starting frame index of this track in the source video.
	 * User can use this to calculate the starting time of this track using the FPS information of the video.
	 * Its default value is -1, meaning the index has not been determined yet.
	 */
	public int startFrameIndex = -1;
	/**
	 * Storing the locations with the BoundingBox class at each moment, sorted by time.
	 */
	public List<BoundingBox> locationSequence = null;
}