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

import java.io.Serializable;

/**
 * The Attribute class stores all the pre-defined attributes of a pedestrian at
 * one moment in a track. In other words, each attribute object correspond to
 * one bounding box in a track.
 *
 * <br>
 * TODO Fill the pre-defined attributes.</br>
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class Attributes implements Serializable {

	private static final long serialVersionUID = -7873269416770994896L;

	/**
	 * Enumeration of possible facing status.
	 * 
	 * @author Ken Yu, CRIPAC, 2016
	 */
	public enum Facing {
		LEFT, RIGHT, FRONT, BACK
	}

	/**
	 * The direction the pedestrian is facing at this moment.
	 */
	public Facing facing;

	/**
	 * Enumeration of possible sexes.
	 * 
	 * @author Ken Yu, CRIPAC, 2016
	 *
	 */
	public enum Sex {
		MALE, FEMALE, UNDETERMINED
	}

	/**
	 * The sex of the pedestrian in the track.
	 */
	public Sex sex;

	/**
	 * This field enables matching an attribute to a track in the same task.
	 * Attribute generating algorithms do not need to fill in this field.
	 */
	public int trackID = -1;

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "|Attributes-track=" + trackID + "-facing=" + facing + "-sex=" + sex + "|";
	}
}