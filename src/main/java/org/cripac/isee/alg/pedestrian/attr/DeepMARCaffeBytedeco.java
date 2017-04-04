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

package org.cripac.isee.alg.pedestrian.attr;

import org.apache.commons.io.IOUtils;
import org.bytedeco.javacpp.caffe;
import org.cripac.isee.alg.CaffeBytedeco;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.AccessDeniedException;
import java.util.Collection;

/**
 * The class DeepMARCaffeBytedeco uses the algorithm proposed in
 * Li, D., Chen, X., Huang, K.:
 * Multi-attribute learning for pedestrian attribute recognition in surveillance scenarios. In: Proc. ACPR (2015)
 * to conduct pedestrian attribute recognizing.
 * <p>
 * Created by ken.yu on 17-1-10.
 */
public final class DeepMARCaffeBytedeco extends CaffeBytedeco implements DeepMARCaffe {

    /**
     * Create an instance of DeepMARCaffeBytedeco. The protocol and weights are directly loaded from local files.
     *
     * @param gpu      index of GPU to use.
     * @param protocol DeepMARCaffeBytedeco protocol file.
     * @param model    DeepMARCaffeBytedeco binary model file.
     * @param logger   external logger.
     */
    public DeepMARCaffeBytedeco(int gpu,
                                @Nonnull File protocol,
                                @Nonnull File model,
                                @Nullable Logger logger) throws FileNotFoundException, AccessDeniedException {
        super(gpu, logger);
        initialize(protocol, model);
    }

    /**
     * Create an instance of DeepMARCaffeBytedeco. The protocol and weights are retrieved from the JAR.
     *
     * @param gpu    index of GPU to use.
     * @param logger logger for outputting debug info.
     */
    public DeepMARCaffeBytedeco(int gpu,
                                @Nullable Logger logger) throws IOException {
        this(gpu, DeepMARCaffe.getDefaultProtobuf(), DeepMARCaffe.getDefaultModel(), logger);
    }

    /**
     * Recognize attributes from a pedestrian tracklet.
     *
     * @param tracklet a pedestrian tracklet.
     * @return attributes of the pedestrian specified by the tracklet.
     */
    @Nonnull
    @Override
    public Attributes recognize(@Nonnull Tracklet tracklet) {
        Collection<Tracklet.BoundingBox> samples = tracklet.getSamples();
        assert samples.size() >= 1;
        //noinspection OptionalGetWithoutIsPresent,ConstantConditions
        return Attributes.div(
                samples.stream().map(this::recognize).reduce(Attributes::add).get(),
                samples.size());
    }

    /**
     * Recognize attributes from a pedestrian bounding box.
     *
     * @param bbox a pedestrian bounding box with patch data.
     * @return attributes of the pedestrian specified by the tracklet.
     */
    @Nonnull
    public Attributes recognize(@Nonnull Tracklet.BoundingBox bbox) {
        float[] pixelFloats = DeepMAR.preprocess(bbox);

        // Put the data into the data blob.
        final caffe.FloatBlob dataBlob = net.blob_by_name("data");
        dataBlob.Reshape(1, 3, INPUT_HEIGHT, INPUT_WIDTH);
//        dataBlob.set_cpu_data(pixelFloats); // Seems like there is some bugs with this function.
        dataBlob.cpu_data().put(pixelFloats);

        // Forward the data.
        net.Forward();

        // Transform result to Attributes and return.
        caffe.FloatBlob outputBlob = net.blob_by_name("fc8");
        final float[] outputArray = new float[outputBlob.count()];
        outputBlob.cpu_data().get(outputArray);
        outputBlob.deallocate();
        dataBlob.deallocate();
        return DeepMAR.fillAttributes(outputArray);
    }
}
