/*
 * This file is part of las-vpe-platform.
 *
 * las-vpe-platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * las-vpe-platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with las-vpe-platform. If not, see <http://www.gnu.org/licenses/>.
 *
 * Created by ken.yu on 17-3-24.
 */
package org.cripac.isee.alg.pedestrian.attr;

import org.apache.commons.io.IOUtils;
import org.cripac.isee.alg.Tensorflow;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.Logger;
import org.tensorflow.Tensor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.FloatBuffer;
import java.util.Collection;

public class DeepMARTensorflow extends Tensorflow implements DeepMAR {
    /**
     * Create an instance of DeepMARTensorflow.
     *
     * @param gpu    index of GPU to use.
     * @param logger logger for outputting debug info.
     */
    public DeepMARTensorflow(String gpu,
                             @Nullable Logger logger) throws IOException {
        this(gpu, getDefaultProtobuf(), getDefaultSessionConfig(), logger);
    }

    private static File getDefaultSessionConfig() throws IOException {
        // Retrieve the file from JAR and store to temporary files.
        InputStream in = DeepMARCaffe.class.getResourceAsStream("/models/DeepMARTensorflow/tf_session_config.pb");
        if (in == null) {
            throw new FileNotFoundException("Cannot find default Tensorflow session configuration in the JAR package.");
        }

        try {
            File tempFile = File.createTempFile("tf_session_config", ".pb");
            tempFile.deleteOnExit();
            try (OutputStream out = new FileOutputStream(tempFile)) {
                IOUtils.copy(in, out);
                return tempFile;
            }
        } finally {
            in.close();
        }
    }

    private static File getDefaultProtobuf() throws IOException {
        // Retrieve the file from JAR and store to temporary files.
        InputStream in = DeepMARCaffe.class.getResourceAsStream("/models/DeepMARTensorflow/DeepMAR_frozen.pb");
        if (in == null) {
            throw new FileNotFoundException("Cannot find default Tensorflow frozen protobuf in the JAR package.");
        }

        try {
            File tempFile = File.createTempFile("DeepMAR_frozen", ".pb");
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
     * Create an instance of DeepMARTensorflow.
     *
     * @param gpu           index of GPU to use.
     * @param frozenPB      frozen graph protobuf of trained DeepMAR.
     * @param sessionConfig serialized session configuration protobuf.
     * @param logger        logger for outputting debug info.
     */
    public DeepMARTensorflow(String gpu,
                             @Nonnull File frozenPB,
                             @Nonnull File sessionConfig,
                             @Nullable Logger logger) throws IOException {
        super(gpu, logger);
        initialize(frozenPB, sessionConfig);
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
        //noinspection OptionalGetWithoutIsPresent
        return Attributes.div(
                samples.stream().map(this::recognize).reduce(Attributes::add).get(),
                samples.size());
    }

    @Nonnull
    public Attributes recognize(@Nonnull Tracklet.BoundingBox bbox) {
        float[] pixelFloats = DeepMAR.pixelFloatsFromBBox(bbox);

        try (Tensor input = Tensor.create(
                // TODO: Confirm the order of height and width parameter.
                new long[]{1, INPUT_HEIGHT, INPUT_WIDTH, 3},
                FloatBuffer.wrap(pixelFloats))) {
            try (Tensor output = session.runner()
                    .feed("data", input)
                    .fetch(graph.operation("fc8/fc8").output(0))
                    .run().get(0)) {
                FloatBuffer floatBuffer = FloatBuffer.allocate(output.numElements());
                output.writeTo(floatBuffer);

                // transform result to Attributes and return.
                return DeepMAR.fillAttributes(floatBuffer.array());
            }
        }
    }
}
