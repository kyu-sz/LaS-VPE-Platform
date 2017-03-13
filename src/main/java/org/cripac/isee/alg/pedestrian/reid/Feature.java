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

/*
 * Feature.java
 *
 *  Created on: Sep 22, 2016
 *      Author: ken
 */

package org.cripac.isee.alg.pedestrian.reid;

/**
 * Base class for features of different lengths.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public abstract class Feature {
    /**
     * @return The length of the feature.
     */
    public abstract int getLength();

    /**
     * @return Number of bytes of the feature vector.
     */
    public abstract int getNumBytes();

    /**
     * @return Feature vector.
     */
    public abstract float[] getVector();

    /**
     * @return Bytes of the feature vector.
     */
    public abstract byte[] getBytes();
}
