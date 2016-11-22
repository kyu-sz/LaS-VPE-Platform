/***********************************************************************
 * This file is part of LaS-VPE Platform.
 *
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 ************************************************************************/

package org.cripac.isee.vpe.debug;

import org.cripac.isee.pedestrian.attr.Attributes;
import org.cripac.isee.pedestrian.attr.Attributes.Facing;
import org.cripac.isee.pedestrian.attr.Attributes.Sex;
import org.cripac.isee.pedestrian.attr.PedestrianAttrRecognizer;
import org.cripac.isee.pedestrian.tracking.Tracklet;

import javax.annotation.Nonnull;
import java.util.Random;

public class FakePedestrianAttrRecognizer extends PedestrianAttrRecognizer {

    private Random random = new Random();

    @Override
    public Attributes recognize(@Nonnull Tracklet tracklet) {
        Attributes attribute = new Attributes();

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

        switch (random.nextInt(3)) {
            case 0:
                attribute.sex = Sex.FEMALE;
                break;
            case 1:
                attribute.sex = Sex.MALE;
                break;
            case 2:
                attribute.sex = Sex.UNDETERMINED;
                break;
        }

        return attribute;
    }

}
