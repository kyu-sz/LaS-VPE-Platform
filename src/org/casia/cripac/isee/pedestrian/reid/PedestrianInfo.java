/***********************************************************************
 * This file is part of LaS-VPE-Platform.
 * 
 * LaS-VPE-Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * LaS-VPE-Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE-Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/
package org.casia.cripac.isee.pedestrian.reid;

import java.io.Serializable;

import org.casia.cripac.isee.pedestrian.attr.Attributes;
import org.casia.cripac.isee.pedestrian.tracking.Track;

/**
 * A wrapper for a track and the attributes of it.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class PedestrianInfo implements Serializable, Cloneable {

	private static final long serialVersionUID = 5845993089063557863L;

	public Track track = null;
	public Attributes attr = null;

	/**
	 * The ID of the pedestrian. If not decided yet, it is left as -1.
	 */
	public int id = -1;

	/**
	 * Extracted feature for comparison, if applicable.
	 */
	public Feature feature = null;

	/**
	 * Constructor with track and attributes specified at the beginning.
	 * 
	 * @param track
	 *            The track of the pedestrian.
	 * @param attr
	 *            Attributes recognized from the pedestrian.
	 */
	public PedestrianInfo(Track track, Attributes attr) {
		this.track = track;
		this.attr = attr;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "|" + track + "|" + attr + "|";
	}
}