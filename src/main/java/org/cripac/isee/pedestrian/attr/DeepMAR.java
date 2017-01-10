package org.cripac.isee.pedestrian.attr;/***********************************************************************
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

import org.apache.commons.lang.NotImplementedException;
import org.bytedeco.javacpp.caffe;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.bytedeco.javacpp.caffe.TEST;

/**
 * Created by ken.yu on 17-1-10.
 */
public final class DeepMAR extends PedestrianAttrRecognizer {

    private final static String model = "DeepMAR.prototxt";
    private final static String weights = "DeepMAR.caffemodel";
    private caffe.FloatNet net = null;

    public DeepMAR(int gpu, Logger logger) {
        if (gpu >= 0) {
            logger.info("Use GPU with device ID " + gpu);
            caffe.Caffe.SetDevice(gpu);
            caffe.Caffe.set_mode(caffe.Caffe.GPU);
        } else {
            logger.info("Use CPU.");
            caffe.Caffe.set_mode(caffe.Caffe.CPU);
        }

        net = new caffe.FloatNet(model, TEST);
        net.CopyTrainedLayersFrom(weights);
    }

    /**
     * Recognize attributes from a track of pedestrian.
     *
     * @param tracklet A pedestrian track.
     * @return The attributes of the pedestrian specified by the track.
     * @throws IOException Exception that might occur during recognition.
     */
    @Override
    public Attributes recognize(@Nonnull Tracklet tracklet) throws IOException {
        Tracklet.BoundingBox bbox = tracklet.locationSequence[tracklet.locationSequence.length >> 1];
        caffe.FloatBlobVector bottomVec = new caffe.FloatBlobVector();
        // TODO: Fill the data in the bbox into the bottomVec.

        caffe.FloatBlobVector result = net.Forward(bottomVec);
        // TODO: Transform result to Attributes.
        throw new NotImplementedException("The DeepMAR is under development!");
    }
}
