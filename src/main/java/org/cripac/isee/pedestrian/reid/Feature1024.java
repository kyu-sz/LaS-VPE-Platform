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

package org.cripac.isee.pedestrian.reid;

import java.nio.ByteBuffer;

/**
 * A 1024-dim float feature of pedestrian for ReID.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public final class Feature1024 extends Feature {

    /**
     * Length of feature vectors.
     */
    public static final int LENGTH = 1024;

    /**
     * Total number of bytes to represent
     */
    public static final int NUM_BYTES = LENGTH * Float.BYTES;

    /**
     * The feature vector.
     */
    private byte[] vectorBytes = null;

    /**
     * Create a feature with no data filled.
     */
    public Feature1024() {
        vectorBytes = new byte[NUM_BYTES];
    }

    /**
     * Create a feature with known feature vector.
     *
     * @param featureVector The feature vector to fill into the new feature. Its length
     *                      should be the same as the predefined length of the Feature
     *                      class.
     */
    public Feature1024(byte[] featureVector) {
        assert (featureVector.length == NUM_BYTES);
        vectorBytes = featureVector.clone();
    }

    /*
     * (non-Javadoc)
     *
     * @see Feature#getLength()
     */
    @Override
    public int getLength() {
        return LENGTH;
    }

    /*
     * (non-Javadoc)
     *
     * @see Feature#getNumBytes()
     */
    @Override
    public int getNumBytes() {
        return LENGTH * Float.BYTES;
    }

    /*
     * (non-Javadoc)
     *
     * @see Feature#getVector()
     */
    @Override
    public float[] getVector() {
        float[] vector = new float[LENGTH];
        for (int i = 0, offset = 0; i < LENGTH; ++i, offset += Float.BYTES) {
            vector[i] = ByteBuffer.wrap(vectorBytes, offset, Float.BYTES).getFloat();
        }
        return vector;
    }

    /*
     * (non-Javadoc)
     *
     * @see Feature#getBytes()
     */
    @Override
    public byte[] getBytes() {
        return vectorBytes;
    }
}
