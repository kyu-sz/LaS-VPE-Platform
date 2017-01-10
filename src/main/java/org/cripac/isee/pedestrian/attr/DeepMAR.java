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

import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import org.apache.commons.lang.NotImplementedException;
import org.bytedeco.javacpp.*;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import java.io.DataInputStream;
import java.io.IOException;

import static org.bytedeco.javacpp.caffe.TEST;
import static org.bytedeco.javacpp.opencv_core.CV_32SC3;
import static org.bytedeco.javacpp.opencv_core.CV_8UC3;

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
        // Process image.
        final Tracklet.BoundingBox bbox = tracklet.locationSequence[tracklet.locationSequence.length >> 1];
        opencv_core.Mat image = new opencv_core.Mat(bbox.height, bbox.width, CV_8UC3);
        image.data(new BytePointer(bbox.patchData));
        image.convertTo(image, CV_32SC3);
        opencv_imgproc.resize(image, image, new opencv_core.Size(224, 224));

        // Get pixel data.
        final int numPixels = image.rows() * image.cols() * image.channels();
        final byte[] pixelFloatBytes = new byte[Float.BYTES * numPixels];
        image.data().get(pixelFloatBytes);
        final DataInputStream pixelFloatStream = new DataInputStream(new ByteInputStream(pixelFloatBytes, pixelFloatBytes.length));
        final float[] pixelFloats = new float[numPixels];
        for (int i = 0; i < numPixels; ++i) {
            pixelFloats[i] = pixelFloatStream.readFloat();
        }
        // TODO: Verify correctness of the prepared pixel floats.

        // Prepare input blob.
        final caffe.FloatBlobVector bottomVec = new caffe.FloatBlobVector();
        final caffe.FloatBlob dataBlob = new caffe.FloatBlob(1, 3, 224, 224);
        dataBlob.set_cpu_data(pixelFloats);
        bottomVec.put(dataBlob);

        // Forward the input blob and get result.
        final caffe.FloatBlobVector result = net.Forward(bottomVec);
        final FloatPointer p = result.get(0).cpu_data();

        // TODO: Transform result to Attributes.
        Attributes attributes = new Attributes();
        attributes.genderFemale = p.get(0);
        throw new NotImplementedException("The DeepMAR is under development!");
    }
}
