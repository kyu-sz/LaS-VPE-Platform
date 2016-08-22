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
package org.casia.cripac.isee.vpe.debug;

import java.util.Random;

import org.casia.cripac.isee.pedestrian.attr.Attributes;
import org.casia.cripac.isee.pedestrian.reid.PedestrianReIDerWithAttr;
import org.casia.cripac.isee.pedestrian.tracking.Track;

/**
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class FakePedestrianReIDerWithAttr extends PedestrianReIDerWithAttr {

	Random rand = new Random();
	
	/* (non-Javadoc)
	 * @see org.casia.cripac.isee.pedestrian.reid.PedestrianReIDerWithAttr#reid(org.casia.cripac.isee.pedestrian.tracking.Track, org.casia.cripac.isee.pedestrian.attr.Attribute)
	 */
	@Override
	public int reid(Track track, Attributes attr) {
		return rand.nextInt(100);
	}

}
