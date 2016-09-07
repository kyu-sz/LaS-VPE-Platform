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
package org.casia.cripac.isee.vpe.debug;

import java.io.IOException;
import java.util.Random;

import org.casia.cripac.isee.pedestrian.reid.PedestrianInfo;
import org.casia.cripac.isee.pedestrian.reid.PedestrianReIDer;

/**
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class FakePedestrianReIDerWithAttr extends PedestrianReIDer {

	Random rand = new Random();

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.casia.cripac.isee.pedestrian.reid.PedestrianReIDerWithAttr#reid(org.
	 * casia.cripac.isee.pedestrian.tracking.Track,
	 * org.casia.cripac.isee.pedestrian.attr.Attribute)
	 */
	@Override
	public int[] reid(PedestrianInfo target) throws IOException {
		int[] rank = new int[10];
		for (int i = 0; i < 10; ++i) {
			rank[i] = rand.nextInt(10000);
		}
		return rank;
	}

}
