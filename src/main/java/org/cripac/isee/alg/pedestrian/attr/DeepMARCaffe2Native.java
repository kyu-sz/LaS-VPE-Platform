/*
 * This file is part of LaS-VPE-Platform.
 *
 * LaS-VPE-Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE-Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE-Platform. If not, see <http://www.gnu.org/licenses/>.
 *
 * Created by ken.yu on 17-3-27.
 */
package org.cripac.isee.alg.pedestrian.attr;

import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.file.AccessDeniedException;
import java.util.Collection;

/**
 * The class DeepMARCaffe2Native recognizes attributes from pedestrian images using native libraries.
 * The recognition functions are thread safe, but cannot benefit from multi-threading.
 * To benefit from multi-threading and multi-GPU,
 * create multiple instances of this class in different threads,
 * each using a different GPU.
 */
public class DeepMARCaffe2Native implements DeepMARCaffe2, BatchRecognizer {
    // Load the DeepMAR native library.
    static {
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(DeepMARCaffe2.class);
        try {
            logger.info("Loading native libraries for DeepMARCaffe2Native from "
                    + System.getProperty("java.library.path"));
            System.loadLibrary("jniDeepMARCaffe2");
            logger.info("Native libraries for DeepMARCaffe2Native successfully loaded!");
        } catch (Throwable t) {
            logger.error("Failed to load native library for DeepMARCaffe2Native", t);
            throw t;
        }
    }

    private long net;
    private final float[] outputBuf = new float[1000];
    private float[][] multiOutputBuf = null;
    private int lastBatchSize = 0;
    private Logger logger;

    /**
     * Initialize the native DeepMAR network.
     *
     * @param gpu       index of GPU to run on.
     * @param pbPath    ASCII path of protocol buffer.
     * @param modelPath ASCII path of Caffe model.
     * @return pointer to the allocated native network.
     */
    private native long initialize(int gpu,
                                   @Nonnull String pbPath,
                                   @Nonnull String modelPath);

    /**
     * Free the network.
     *
     * @param p the pointer to the network.
     */
    private native void free(long p);

    /**
     * Recognize attributes from an image using the network.
     *
     * @param net        the pointer to the network.
     * @param pixelBytes pixel bytes retrieved from the image.
     * @param outputBuf  buffer for returning the FC8 result.
     */
    public native void recognize(long net,
                                 @Nonnull float[] pixelBytes,
                                 @Nonnull float[] outputBuf);

    /**
     * Recognize attributes from a batch of images using the network.
     *
     * @param net        the pointer to the network.
     * @param pixelBytes pixel bytes retrieved from each of the images in the batch.
     * @param outputBuf  buffer for returning the FC8 results for each of the images in the batch.
     */
    public native void recognize(long net,
                                 @Nonnull float[][] pixelBytes,
                                 @Nonnull float[][] outputBuf);

    /**
     * Free the network.
     *
     * @throws Throwable on failure finalizing the super class.
     */
    @Override
    protected void finalize() throws Throwable {
        free(net);
        super.finalize();
    }

    /**
     * Create an instance of DeepMARCaffe2Native.
     * The protocol and weights are directly loaded from local files.
     *
     * @param gpu         index of GPU to use.
     *                    If multiple GPU IDs are provided (separated by comma), randomly select one.
     * @param init_net    DeepMARCaffe2Native init net protobuf file.
     * @param predict_net DeepMARCaffe2Native predict net protobuf file.
     */
    public DeepMARCaffe2Native(String gpu,
                               @Nonnull File init_net,
                               @Nonnull File predict_net,
                               @Nonnull Logger logger)
            throws FileNotFoundException, AccessDeniedException, CharacterCodingException {
        this.logger = logger;
        this.logger.debug("Initializing DeepMARCaffe2Native with "
                + init_net.getPath() + "(" + (init_net.length() / 1024) + "kb) and "
                + predict_net.getPath() + "(" + (predict_net.length() / 1024) + "kb)");
        if (!init_net.exists()) {
            throw new FileNotFoundException("Cannot find " + init_net.getPath());
        }
        if (!predict_net.exists()) {
            throw new FileNotFoundException("Cannot find " + predict_net.getPath());
        }
        if (!init_net.canRead()) {
            throw new AccessDeniedException("Cannot read " + init_net.getPath());
        }
        if (!predict_net.canRead()) {
            throw new AccessDeniedException("Cannot read " + predict_net.getPath());
        }

        net = initialize(
                DeepMAR.randomlyPickGPU(gpu),
                init_net.getPath(),
                predict_net.getPath());
        this.logger.debug("DeepMARCaffe2Native initialized!");
    }

    /**
     * Create an instance of DeepMARCaffe2Native. The protocol and weights are retrieved from the JAR.
     *
     * @param gpu index of GPU to use.
     *            If multiple GPU IDs are provided (separated by comma), randomly select one.
     */
    public DeepMARCaffe2Native(String gpu,
                               @Nonnull Logger logger) throws IOException {
        this(gpu, DeepMARCaffe2.getInitNetProtobuf(), DeepMARCaffe2.getPredictNetModel(), logger);
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
                samples.stream()
                        .map(this::recognize)
                        .reduce(Attributes::add)
                        .get(),
                samples.size());
    }

    /**
     * Recognize attributes from a pedestrian bounding box image.
     *
     * @param bbox the bounding box containing the target pedestrian image.
     * @return attributes of the pedestrian in the image.
     */
    @Nonnull
    public synchronized Attributes recognize(@Nonnull Tracklet.BoundingBox bbox) {
        recognize(net, DeepMAR.preprocess(bbox), outputBuf);
        return DeepMAR.fillAttributes(outputBuf);
    }

    /**
     * Recognize attributes from a batch of pedestrian bounding box images.
     *
     * @param bboxes a batch of bounding boxes containing the images.
     * @return attributes of the pedestrians in the images.
     */
    @Nonnull
    @Override
    public synchronized Attributes[] recognize(@Nonnull Tracklet.BoundingBox[] bboxes) {
        synchronized (this) {
            float[][] data = new float[bboxes.length][];
            for (int i = 0; i < bboxes.length; ++i) {
                assert bboxes[i] != null;
                data[i] = DeepMAR.preprocess(bboxes[i]);
            }
            if (lastBatchSize != bboxes.length) {
                lastBatchSize = bboxes.length;
                multiOutputBuf = new float[lastBatchSize][];
                for (int i = 0; i < lastBatchSize; ++i) {
                    multiOutputBuf[i] = new float[1000];
                }
            }
            recognize(net, data, multiOutputBuf);
            Attributes[] attributes = new Attributes[lastBatchSize];
            for (int i = 0; i < lastBatchSize; ++i) {
                attributes[i] = DeepMAR.fillAttributes(multiOutputBuf[i]);
            }
            return attributes;
        }
    }
}
