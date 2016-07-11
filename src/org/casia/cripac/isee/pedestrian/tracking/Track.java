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
	
	public class BoundingBox implements Serializable {
		
		private static final long serialVersionUID = -5261437055893590056L;
		
		public int x;
		public int y;
		public int width;
		public int height;
	}
	
	private static final long serialVersionUID = -6133927313545472821L;
	
	public String videoURL;
	public int startFrameIndex;
	public List<BoundingBox> locationSequence;
}