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

import org.casia.cripac.isee.pedestrian.attr.Attribute;
import org.casia.cripac.isee.pedestrian.attr.PedestrianAttrRecognizer;
import org.casia.cripac.isee.pedestrian.attr.Attribute.Facing;
import org.casia.cripac.isee.pedestrian.tracking.Track;

public class FakePedestrianAttrRecognizer extends PedestrianAttrRecognizer {

	Random random = new Random();
	
	@Override
	public Attribute recognize(Track track) {
		Attribute attribute = new Attribute();
		
		switch (random.nextInt(4)) {
		case 0:
			attribute.facing = Facing.FRONT;
			break;
		case 1:
			attribute.facing = Facing.BACK;
			break;
		case 2:
			attribute.facing = Facing.LEFT;
			break;
		case 3:
			attribute.facing = Facing.RIGHT;
			break;
		}
		
		return attribute;
	}
	
}
