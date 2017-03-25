package org.cripac.isee.alg;

import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.caffe;
import org.bytedeco.javacpp.opencv_core;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.AccessDeniedException;

import static org.bytedeco.javacpp.caffe.TEST;

/**
 * Base class of classes using Caffe.
 * Created by Ken Yu on 2017/3/7.
 */
public class Caffe {
    static {
        Loader.load(opencv_core.class);
        Loader.load(caffe.class);
    }

    /**
     * Instance of Caffe.
     */
    protected caffe.FloatNet net = null;

    protected Logger logger;

    /**
     * Initialize Caffe with protocol and pre-trained model.
     *
     * @param protobuf Caffe protocol buffer file.
     * @param model    Caffe model file.
     */
    protected void initialize(@Nonnull File protobuf,
                              @Nonnull File model) throws FileNotFoundException, AccessDeniedException {
        if (!protobuf.exists()) {
            throw new FileNotFoundException("Cannot find Caffe protocol from " + protobuf.getAbsolutePath());
        }
        if (!protobuf.canRead()) {
            throw new AccessDeniedException("Cannot read Caffe protocol from " + protobuf.getAbsolutePath());
        }
        logger.info("Loading Caffe protocol from " + protobuf.getAbsolutePath()
                + " (" + (protobuf.length() / 1024) + "kb)");
        net = new caffe.FloatNet(protobuf.getAbsolutePath(), TEST);

        if (!model.exists()) {
            throw new FileNotFoundException("Cannot find Caffe model from " + model.getAbsolutePath());
        }
        if (!model.canRead()) {
            throw new AccessDeniedException("Cannot read Caffe model from " + model.getAbsolutePath());
        }
        logger.info("Loading Caffe model from " + model.getAbsolutePath() + " (" + (model.length() / 1024) + "kb)");
        net.CopyTrainedLayersFrom(model.getAbsolutePath());

        this.logger.debug("Caffe initialized!");
    }

    /**
     * Create an instance of DeepMARCaffe.
     *
     * @param gpu index of GPU to use.
     */
    protected Caffe(int gpu) {
        this(gpu, null);
    }

    /**
     * Create an instance of DeepMARCaffe.
     *
     * @param gpu    index of GPU to use.
     * @param logger logger for outputting debug info.
     */
    protected Caffe(int gpu,
                    @Nullable Logger logger) {
        if (logger == null) {
            this.logger = new ConsoleLogger();
        } else {
            this.logger = logger;
        }

        if (gpu >= 0) {
            this.logger.info("Use GPU with device ID " + gpu);
            caffe.Caffe.SetDevice(gpu);
            caffe.Caffe.set_mode(caffe.Caffe.GPU);
        } else {
            this.logger.info("Use CPU.");
            caffe.Caffe.set_mode(caffe.Caffe.CPU);
        }
        this.logger.debug("Caffe mode and device set!");
    }
}
