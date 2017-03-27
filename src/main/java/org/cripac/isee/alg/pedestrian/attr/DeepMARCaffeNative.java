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

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.Collection;

public class DeepMARCaffeNative implements DeepMARCaffe {
    static {
        System.out.println("Loading native libraries for DeepMARCaffeNative from "
                + System.getProperty("java.library.path"));
        System.loadLibrary("deepmar_caffe_jni");
        System.out.println("Native libraries for DeepMARCaffeNative successfully loaded!");
    }

    private long net;

    private native long initialize(int gpu,
                                   @Nonnull char[] protocolPath,
                                   @Nonnull char[] modelPath);

    private native void free(long p);

    public native float[] recognize(long net,
                                    @Nonnull float[] pixelBytes);

    @Override
    protected void finalize() throws Throwable {
        free(net);
        super.finalize();
    }

    /**
     * Create an instance of DeepMARCaffeNative. The protocol and weights are directly loaded from local files.
     *
     * @param gpu      index of GPU to use.
     * @param protocol DeepMARCaffeNative protocol file.
     * @param model    DeepMARCaffeNative binary model file.
     */
    public DeepMARCaffeNative(int gpu,
                              @Nonnull File protocol,
                              @Nonnull File model)
            throws FileNotFoundException, AccessDeniedException {
        net = initialize(gpu, protocol.getPath().toCharArray(), model.getPath().toCharArray());
    }

    /**
     * Create an instance of DeepMARCaffeNative. The protocol and weights are retrieved from the JAR.
     *
     * @param gpu index of GPU to use.
     */
    public DeepMARCaffeNative(int gpu) throws IOException {
        this(gpu, DeepMARCaffe.getDefaultProtobuf(), DeepMARCaffe.getDefaultModel());
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
    public Attributes recognize(@Nonnull Tracklet.BoundingBox bbox) {
        return DeepMAR.fillAttributes(recognize(net, DeepMAR.preprocess(bbox)));
    }
}
