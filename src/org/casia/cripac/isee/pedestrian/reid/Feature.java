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

/**
 * Feature of pedestrian for ReID.
 * 
 * @author Ken Yu, CRIPAC, 2016
 *
 */
public class Feature {

	/**
	 * Length of feature vectors.
	 */
	public static final int LENGTH = 128;

	/**
	 * The feature vector.
	 */
	public byte[] vector = null;

	/**
	 * Create a feature with no data filled.
	 */
	public Feature() {
		vector = new byte[LENGTH];
	}

	/**
	 * Create a feature with known feature vector.
	 * 
	 * @param featureVector
	 *            The feature vector to fill into the new feature. Its length
	 *            should be the same as the predefined length of the Feature
	 *            class.
	 */
	public Feature(byte[] featureVector) {
		assert (featureVector.length == LENGTH);
		vector = featureVector.clone();
	}
}
