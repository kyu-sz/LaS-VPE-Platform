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

import java.io.IOException;

/**
 * The PedestrianReIDer class is the super class for any pedestrian ReID
 * classes.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public abstract class PedestrianReIDer {

	/**
	 * Perform ReID with a target pedestrian.
	 * 
	 * @param pedestrian
	 *            The target pedestrian.
	 * @return A rank of possible IDs of the pedestrian.
	 * @throws IOException
	 */
	public abstract int[] reid(PedestrianInfo pedestrian) throws IOException;
}
