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

package org.casia.cripac.isee.pedestrian.attr;

import org.casia.cripac.isee.pedestrian.tracking.Track;

/**
 * The PedestrianAttrRecognizer class is the base class of all pedestrian attribute recognizing classes.
 * Any subclass is required to implement a simple 'recognize' method, which takes in a track and returns an attribute.
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public abstract class PedestrianAttrRecognizer {
	
	/**
	 * Recognize attributes from a track of pedestrian. 
	 * @param track		A pedestrian track.
	 * @return			The attributes of the pedestrian specified by the track.
	 */
	public abstract Attribute recognize(Track track);
}
