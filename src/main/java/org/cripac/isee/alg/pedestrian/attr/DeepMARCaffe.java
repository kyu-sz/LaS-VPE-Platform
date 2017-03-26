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
import org.cripac.isee.alg.Caffe;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.AccessDeniedException;
import java.util.Collection;

/**
 * The class DeepMARCaffe uses the algorithm proposed in
 * Li, D., Chen, X., Huang, K.:
 * Multi-attribute learning for pedestrian attribute recognition in surveillance scenarios. In: Proc. ACPR (2015)
 * to conduct pedestrian attribute recognizing.
 * <p>
 * Created by ken.yu on 17-1-10.
 */
public final class DeepMARCaffe extends Caffe implements DeepMAR {

    /**
     * Create an instance of DeepMARCaffe. The protocol and weights are directly loaded from local files.
     *
     * @param gpu      index of GPU to use.
     * @param protocol DeepMARCaffe protocol file.
     * @param model    DeepMARCaffe binary model file.
     * @param logger   external logger.
     */
    public DeepMARCaffe(int gpu,
                        @Nonnull File protocol,
                        @Nonnull File model,
                        @Nullable Logger logger) throws FileNotFoundException, AccessDeniedException {
        super(gpu, logger);
        initialize(protocol, model);
    }

    /**
     * Create an instance of DeepMARCaffe. The protocol and weights are retrieved from the JAR.
     *
     * @param gpu    index of GPU to use.
     * @param logger logger for outputting debug info.
     */
    public DeepMARCaffe(int gpu,
                        @Nullable Logger logger) throws IOException {
        this(gpu, getDefaultProtobuf(), getDefaultModel(), logger);
    }

    private static File getDefaultProtobuf() throws IOException {
        // Retrieve the file from JAR and store to temporary files.
        InputStream in = DeepMARCaffe.class.getResourceAsStream("/models/DeepMARCaffe/DeepMAR.prototxt");
        if (in == null) {
            throw new FileNotFoundException("Cannot find default Caffe protocol buffer in the JAR package.");
        }

        try {
            File tempFile = File.createTempFile("DeepMAR", ".prototxt");
            tempFile.deleteOnExit();
            try (OutputStream out = new FileOutputStream(tempFile)) {
                IOUtils.copy(in, out);
                return tempFile;
            }
        } finally {
            in.close();
        }
    }

    private static File getDefaultModel() throws IOException {
        // Retrieve the file from JAR and store to temporary files.
        InputStream in = DeepMARCaffe.class.getResourceAsStream("/models/DeepMARCaffe/DeepMAR.caffemodel");
        if (in == null) {
            throw new FileNotFoundException("Cannot find default Caffe model in the JAR package.");
        }

        try {
            File tempFile = File.createTempFile("DeepMAR", ".caffemodel");
            tempFile.deleteOnExit();
            try (OutputStream out = new FileOutputStream(tempFile)) {
                IOUtils.copy(in, out);
                return tempFile;
            }
        } finally {
            in.close();
        }
    }

    /**
     * Recognize attributes from a pedestrian tracklet.
     *
     * @param tracklet tracklet of the pedestrian.
     * @return the attributes of the pedestrian recognized from the tracklet.
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

    @Nonnull
    public Attributes recognize(@Nonnull Tracklet.BoundingBox bbox) {
        float[] pixelFloats = DeepMAR.pixelFloatsFromBBox(bbox);

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
