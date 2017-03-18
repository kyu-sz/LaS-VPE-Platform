package org.cripac.isee.alg;

import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.caffe;
import org.bytedeco.javacpp.opencv_core;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;

import static org.bytedeco.javacpp.caffe.TEST;

/**
 * Created by 铠 on 2017/3/7.
 */
public class Caffe {
    static {
        Loader.load(opencv_core.class);
        Loader.load(caffe.class);
    }

    /**
     * Instance of Cafre.
     */
    protected caffe.FloatNet net = null;

    protected Logger logger;

    private void setupCaffe(int gpu) {
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

    private void initialize(@Nonnull String protocolPath,
                            @Nonnull String weightsPath) {
        logger.info("Loading Caffe protocol from " + new File(protocolPath).getAbsolutePath());
        net = new caffe.FloatNet(protocolPath, TEST);
        logger.info("Loading Caffe weights from " + new File(weightsPath).getAbsolutePath());
        net.CopyTrainedLayersFrom(weightsPath);
        this.logger.debug("Caffe initialized!");
    }

    /**
     * Create an instance of DeepMAR. The protocol and weights are directly loaded from local files.
     *
     * @param gpu          The GPU to use.
     * @param protocolPath Path of the DeepMAR protocol file.
     * @param weightsPath  Path of the binary weights model of DeepMAR.
     * @param logger       An external logger.
     */
    protected Caffe(int gpu,
                    @Nonnull String protocolPath,
                    @Nonnull String weightsPath,
                    @Nullable Logger logger) {
        if (logger == null) {
            this.logger = new ConsoleLogger();
        } else {
            this.logger = logger;
        }

        setupCaffe(gpu);
        initialize(protocolPath, weightsPath);
    }
}
