package org.cripac.isee.alg.pedestrian.attr;/*
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

import org.apache.commons.io.IOUtils;
import org.bytedeco.javacpp.tensorflow;
import org.cripac.isee.alg.Tensorflow;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.Logger;

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
        this(gpu, getDefaultProtobuf(), getDefaultModel(), logger);
    }

    private static File getDefaultProtobuf() throws IOException {
        // Retrieve the file from JAR and store to temporary files.
        InputStream in = DeepMARCaffe.class.getResourceAsStream("/models/DeepMARTensorflow/DeepMAR.pb");
        if (in == null) {
            throw new FileNotFoundException("Cannot find default Tensorflow protocol buffer in the JAR package.");
        }

        try {
            File tempFile = File.createTempFile("DeepMAR", ".proto");
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
        InputStream in = DeepMARCaffe.class.getResourceAsStream("/models/DeepMARTensorflow/DeepMAR.ckpt");
        if (in == null) {
            throw new FileNotFoundException("Cannot find default Tensorflow protocol buffer in the JAR package.");
        }

        try {
            File tempFile = File.createTempFile("DeepMAR", ".tfmodel");
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
     * @param gpu    index of GPU to use.
     * @param logger logger for outputting debug info.
     */
    public DeepMARTensorflow(String gpu,
                             @Nonnull File protocol,
                             @Nonnull File model,
                             @Nullable Logger logger) throws FileNotFoundException {
        super(gpu, logger);
        initialize(protocol, model);
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

        // try to predict for two (2) sets of inputs.
        tensorflow.Tensor inputs = new tensorflow.Tensor(tensorflow.DT_FLOAT,
                new tensorflow.TensorShape(1, 3, INPUT_HEIGHT, INPUT_WIDTH));
        FloatBuffer x = inputs.createBuffer();
        x.put(pixelFloats);

        tensorflow.TensorVector outputs = new tensorflow.TensorVector();

        // to predict each time, pass in values for placeholders
        outputs.resize(0);
        tensorflow.Status status = session.Run(
                new tensorflow.StringTensorPairVector(
                        new String[]{"data"},
                        new tensorflow.Tensor[]{inputs}),
                new tensorflow.StringVector("fc8"),
                new tensorflow.StringVector(),
                outputs);
        if (!status.ok()) {
            throw new RuntimeException(status.error_message().getString());
        }

        // get back the predicted value from outputs
        FloatBuffer output = outputs.get(0).createBuffer();

        // transform result to Attributes and return.
        return DeepMAR.fillAttributes(output.array());
    }
}
