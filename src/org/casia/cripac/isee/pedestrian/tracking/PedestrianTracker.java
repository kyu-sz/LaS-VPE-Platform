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

/**
 * The PedestrianTracker class is the base class of all pedestrian tracking
 * classes. Any subclass is required to implement a simple 'track' method, which
 * takes in a URL of a video and returns a set of track.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public abstract class PedestrianTracker {

	/**
	 * Read a video from a URL, and perform pedestrian tracking on it. TODO One
	 * might change this to directly takes in a byte array, representing the
	 * data of the video.
	 * 
	 * @param videoURL
	 *            The URL at which the video is stored.
	 * @return A set of tracks of pedestrians.
	 */
	public abstract Track[] track(String videoURL);
}
