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

import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import java.nio.ByteBuffer;

/**
 * The mscan feature of pedestrian for ReID.
 *
 * @author da.li, CRIPAC, 2017
 */
public final class FeatureMSCAN extends Feature {

    private static final long serialVersionUID = 1L;

    /**
     * Length of feature vectors.
     */
    public final int LENGTH = 128;

    /**
     * Total number of bytes to represent
     */
    public final int NUM_BYTES = LENGTH * Float.BYTES;

    /**
     * The feature vector.
     */
    private byte[] vectorBytes = null;

    /**
     * Create a feature with no data filled.
     */
    public FeatureMSCAN() {
        vectorBytes = new byte[NUM_BYTES];
    }

    /**
     * Create a feature with known feature vector.
     *
     * @param featureVector The feature vector to fill into the new feature. Its length
     *                      should be the same as the predefined length of the Feature
     *                      class.
     */
    public FeatureMSCAN(float[] featureVector) {
        assert (featureVector.length == LENGTH);
        vectorBytes = new byte[NUM_BYTES];
        int idx = 0;
        for (int i = 0; i < LENGTH; ++i) {
            putFloat(vectorBytes, featureVector[i], idx);
            idx = idx + Float.BYTES;
        }
    }
    public FeatureMSCAN(byte[] featureVector) {
        assert (featureVector.length == NUM_BYTES);
        vectorBytes = featureVector.clone();
    }
    public FeatureMSCAN(float[] featureVector, Tracklet.Identifier id) {
        this(featureVector);
        this.trackletID = id;
    }
    public FeatureMSCAN(byte[] featureVector, Tracklet.Identifier id) {
        this(featureVector);
        this.trackletID = id;
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

    /**
     * float to byte[]
     */
    private void putFloat(byte[] byteArray, float valFloat, int idx) {
        byte[] bytes = ByteBuffer.allocate(Float.BYTES).putFloat(valFloat).array();
        for (int i = 0; i < Float.BYTES; ++i) {
            byteArray[idx+i] = bytes[i];
        }
    }
}
