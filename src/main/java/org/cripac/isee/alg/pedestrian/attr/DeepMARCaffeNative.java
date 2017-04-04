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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DeepMARCaffeNative implements DeepMARCaffe {
    static {
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(DeepMARCaffe.class);
        try {
            logger.info("Loading native libraries for DeepMARCaffeNative from "
                    + System.getProperty("java.library.path"));
            System.loadLibrary("jniDeepMARCaffe");
            logger.info("Native libraries for DeepMARCaffeNative successfully loaded!");
        } catch (Throwable t) {
            logger.error("Failed to load native library for DeepMARCaffeNative", t);
            throw t;
        }
    }

    private long net;
    private final float[] outputBuf = new float[1024];
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
                                   @Nonnull byte[] pbPath,
                                   @Nonnull byte[] modelPath);

    private native void free(long p);

    public native void recognize(long net,
                                 @Nonnull float[] pixelBytes,
                                 @Nonnull float[] outputBuf);

    @Override
    protected void finalize() throws Throwable {
        free(net);
        super.finalize();
    }

    /**
     * Convert a UTF-16 character array (Java default) to ASCII character array.
     * The new character array has exactly the same length.
     * Characters not recognizable in ASCII are represented by '?'.
     *
     * @param str a UTF-16 character array.
     * @return an ASCII character array.
     */
    @Nonnull
    private byte[] toASCII(@Nonnull char[] str) throws CharacterCodingException {
        byte[] ascii = new byte[str.length + 1];
        for (int i = 0; i < str.length; ++i) {
            char ch = str[i];
            if (ch > 0xFF) {
                throw new CharacterCodingException();
            }
            ascii[i] = (byte) ch;
        }
        ascii[str.length] = '\0';
        return ascii;
    }

    /**
     * Create an instance of DeepMARCaffeNative.
     * The protocol and weights are directly loaded from local files.
     *
     * @param gpu   index of GPU to use.
     * @param pb    DeepMARCaffeNative protocol buffer file.
     * @param model DeepMARCaffeNative binary model file.
     */
    public DeepMARCaffeNative(int gpu,
                              @Nonnull File pb,
                              @Nonnull File model,
                              @Nonnull Logger logger)
            throws FileNotFoundException, AccessDeniedException, CharacterCodingException {
        this.logger = logger;
        this.logger.debug("Initializing DeepMARCaffeNative with "
                + pb.getPath() + "(" + (pb.length() / 1024) + "kb) and "
                + model.getPath() + "(" + (model.length() / 1024) + "kb)");
        if (!pb.exists()) {
            throw new FileNotFoundException("Cannot find " + pb.getPath());
        }
        if (!model.exists()) {
            throw new FileNotFoundException("Cannot find " + model.getPath());
        }
        if (!pb.canRead()) {
            throw new AccessDeniedException("Cannot read " + pb.getPath());
        }
        if (!model.canRead()) {
            throw new AccessDeniedException("Cannot read " + model.getPath());
        }
        net = initialize(gpu,
                toASCII(pb.getPath().toCharArray()),
                toASCII(model.getPath().toCharArray()));
        this.logger.debug("DeepMARCaffeNative initialized!");
    }

    /**
     * Create an instance of DeepMARCaffeNative. The protocol and weights are retrieved from the JAR.
     *
     * @param gpu index of GPU to use.
     */
    public DeepMARCaffeNative(int gpu,
                              @Nonnull Logger logger) throws IOException {
        this(gpu, DeepMARCaffe.getDefaultProtobuf(), DeepMARCaffe.getDefaultModel(), logger);
    }

    /**
     * Recognize attributes from a track of pedestrian.
     *
     * @param tracklet A pedestrian track.
     * @return The attributes of the pedestrian specified by the track.
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

    @Nonnull
    public synchronized Attributes recognize(@Nonnull Tracklet.BoundingBox bbox) {
        logger.debug("Recognizing...");
        recognize(net, DeepMAR.preprocess(bbox), outputBuf);
        logger.debug("Recognition finished!");
        return DeepMAR.fillAttributes(outputBuf);
    }
}
