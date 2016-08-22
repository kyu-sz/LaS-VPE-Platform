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
package org.casia.cripac.isee.pedestrian.reid;

import org.casia.cripac.isee.pedestrian.attr.Attributes;
import org.casia.cripac.isee.pedestrian.tracking.Track;

/**
 * The PedestrianReIDerWithAttr class is the base class for pedestrian
 * re-identifiers using attributes. Any subclass is required to implement a
 * simple 'reid' method, which takes in a track and attributes of it, and
 * returns the ID of the pedestrian.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public abstract class PedestrianReIDerWithAttr {
	
	/**
	 * Perform re-identification on a track with attributes recognized on it.
	 * @param track	A track of a pedestrian.
	 * @param attr	Attributes recognized from the pedestrian in the track.
	 * @return		The ID of the pedestrian.
	 */
	public abstract int reid(Track track, Attributes attr);
}
