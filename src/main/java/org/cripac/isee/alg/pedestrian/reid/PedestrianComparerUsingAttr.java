/*
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
 */

package org.cripac.isee.alg.pedestrian.reid;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * The PedestrianComparerWithAttr class is the base class for pedestrian
 * comparer using attributes for pedestrian ReID. Any subclass is required to
 * implement a simple 'compare' method.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public interface PedestrianComparerUsingAttr {

    /**
     * Compare two pedestrians each given in a track with attributes.
     *
     * @param personA The first pedestrian.
     * @param personB The second pedestrian.
     * @return The similarity between them.
     * @throws Exception
     */
    float compare(@Nonnull PedestrianInfo personA,
                  @Nonnull PedestrianInfo personB) throws Exception;
}