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
 * Created by ken.yu on 17-3-20.
 */
package org.cripac.isee.alg;

import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.tensorflow;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;

import static org.bytedeco.javacpp.tensorflow.*;

public class Tensorflow {

    protected Logger logger;
    protected Session session;

    /**
     * Initialize Tensorflow with protocol and pre-trained model.
     *
     * @param graphPB Tensorflow graph protocol buffer file.
     * @param model   Tensorflow model file.
     */
    protected void initialize(@Nonnull File graphPB,
                              @Nonnull File model) throws FileNotFoundException {
        if (!graphPB.exists()) {
            throw new FileNotFoundException("Graph protocol buffer not found at " + graphPB.getAbsolutePath());
        }
        logger.info("Loading Tensorflow graph protocol from " + graphPB.getAbsolutePath());
        session = new Session(new SessionOptions());
        GraphDef def = new GraphDef();
        ReadBinaryProto(Env.Default(), graphPB.getAbsolutePath(), def);
        Status status = session.Create(def);
        if (!status.ok()) {
            throw new RuntimeException(status.error_message().getString());
        }

        if (!model.exists()) {
            throw new FileNotFoundException("Model not found at " + model.getAbsolutePath());
        }
        logger.info("Loading Tensorflow model from " + model.getAbsolutePath());
        Tensor fn = new Tensor(tensorflow.DT_STRING, new TensorShape(1));
        StringArray a = fn.createStringArray();
        a.position(0).put(model.getAbsolutePath());
        status = session.Run(
                new StringTensorPairVector(
                        new String[]{"save/Const:0"},
                        new Tensor[]{fn}),
                new StringVector(),
                new StringVector("save/restore_all"),
                new TensorVector());
        if (!status.ok()) {
            throw new RuntimeException(status.error_message().getString());
        }

        this.logger.debug("Tensorflow initialized!");
    }

    /**
     * Create an instance of Tensorflow.
     *
     * @param gpu    index of GPU to use.
     * @param logger logger for outputting debug info.
     */
    protected Tensorflow(String gpu,
                         @Nullable Logger logger) {
        Loader.load(opencv_core.class);
        Loader.load(tensorflow.class);

        if (logger == null) {
            this.logger = new ConsoleLogger();
        } else {
            this.logger = logger;
        }
    }
}
