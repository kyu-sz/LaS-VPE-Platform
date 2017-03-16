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

import com.google.gson.Gson;
import org.bytedeco.javacpp.*;
import org.cripac.isee.alg.Caffe;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.bytedeco.javacpp.opencv_core.*;

/**
 * The class DeepMAR uses the algorithm proposed in
 * Li, D., Chen, X., Huang, K.:
 * Multi-attribute learning for pedestrian attribute recognition in surveillance scenarios. In: Proc. ACPR (2015)
 * to conduct pedestrian attribute recognizing.
 * <p>
 * Created by ken.yu on 17-1-10.
 */
public final class DeepMAR extends Caffe implements PedestrianAttrRecognizer {
    private final static FloatPointer pMean;
    private final static FloatPointer pRegCoeff;
    private final static DoublePointer pScale;

    private final static float MEAN_PIXEL = 128;
    private final static float REG_COEFF = 1.0f / 256;
    private final static String[] ATTR_LIST;
    private static File tempProtocolFile;
    private static File tempWeightsFile;

    static {
        // Retrieve model files from JAR and store to temporary files.
        try {
            tempProtocolFile = File.createTempFile("DeepMAR", ".prototxt");
            tempWeightsFile = File.createTempFile("DeepMAR", ".caffemodel");
            tempProtocolFile.deleteOnExit();
            tempWeightsFile.deleteOnExit();
            Files.copy(DeepMAR.class.getResourceAsStream("/models/DeepMAR/DeepMAR.prototxt"),
                    tempProtocolFile.toPath(),
                    REPLACE_EXISTING);
            Files.copy(DeepMAR.class.getResourceAsStream("/models/DeepMAR/DeepMAR.caffemodel"),
                    tempWeightsFile.toPath(),
                    REPLACE_EXISTING);
        } catch (IOException | NullPointerException e) {
            tempWeightsFile = null;
            tempProtocolFile = null;
        }

        final float[] meanBuf = new float[1];
        final float[] regBuf = new float[1];
        final double[] scaleBuf = new double[1];
        meanBuf[0] = MEAN_PIXEL;
        regBuf[0] = REG_COEFF;
        scaleBuf[0] = 1;
        pMean = new FloatPointer(meanBuf);
        pRegCoeff = new FloatPointer(regBuf);
        pScale = new DoublePointer(scaleBuf);
    }

    static {
        ATTR_LIST = new String[]{
                "action_pulling",
                "lower_green",
                "gender_female",
                "upper_cotton",
                "accessory_other",
                "occlusion_accessory",
                "upper_other_color",
                "shoes_casual",
                "shoes_white",
                "lower_pants",
                "shoes_boot",
                "age_60",
                "weight_little_thin",
                "head_shoulder_mask",
                "upper_vest",
                "lower_white",
                "upper_black",
                "upper_white",
                "upper_shirt",
                "upper_silvery",
                "role_client",
                "upper_brown",
                "action_nipthing",
                "shoes_silver",
                "accessory_waistbag",
                "accessory_handbag",
                "action_picking",
                "shoes_black",
                "occlusion_down",
                "shoes_yellow",
                "gender_other",
                "accessory_shoulderbag",
                "upper_cotta",
                "occlusion_right",
                "action_pushing",
                "shoes_green",
                "action_armstretching",
                "shoes_other",
                "shoes_red",
                "lower_mix_color",
                "occlusion_left",
                "view_angle_left",
                "shoes_sport",
                "lower_gray",
                "upper_other",
                "accessory_kid",
                "head_shoulder_sunglasses",
                "lower_silver",
                "accessory_cart",
                "age_16",
                "hair_style_null",
                "upper_hoodie",
                "shoes_mix_color",
                "upper_green",
                "accessory_backpack",
                "age_older_60",
                "shoes_cloth",
                "action_chatting",
                "shoes_purple",
                "upper_suit",
                "lower_black",
                "lower_tight_pants",
                "occlusion_up",
                "action_holdthing",
                "lower_pink",
                "action_other",
                "lower_jean",
                "hair_style_long",
                "upper_red",
                "role_uniform",
                "lower_short_pants",
                "lower_one_piece",
                "lower_blue",
                "upper_tshirt",
                "upper_purple",
                "upper_pink",
                "action_lying",
                "shoes_pink",
                "shoes_shandle",
                "shoes_leather",
                "occlusion_environment",
                "view_angle_right",
                "shoes_other_color",
                "head_shoulder_with_hat",
                "age_30",
                "shoes_gray",
                "accessory_paperbag",
                "shoes_brown",
                "action_crouching",
                "lower_purple",
                "weight_very_thin",
                "shoes_blue",
                "action_gathering",
                "weight_normal",
                "action_running",
                "view_angle_front",
                "accessory_plasticbag",
                "head_shoulder_black_hair",
                "accessory_box",
                "lower_long_skirt",
                "shoes_orange",
                "weight_little_fat",
                "head_shoulder_scarf",
                "lower_other_color",
                "upper_jacket",
                "upper_gray",
                "lower_short_skirt",
                "age_45",
                "lower_skirt",
                "upper_sweater",
                "lower_brown",
                "lower_yellow",
                "occlusion_object",
                "upper_orange",
                "gender_male",
                "view_angle_back",
                "upper_blue",
                "lower_red",
                "head_shoulder_glasses",
                "upper_mix_color",
                "lower_orange",
                "upper_yellow",
                "weight_very_fat",
                "action_calling",
                "occlusion_other"};
    }

    private final int INPUT_WIDTH = 227;
    private final int INPUT_HEIGHT = 227;

    /**
     * Create an instance of DeepMAR. The protocol and weights are directly loaded from local files.
     *
     * @param gpu          the GPU to use.
     * @param protocolPath path of the DeepMAR protocol file.
     * @param weightsPath  path of the binary weights model of DeepMAR.
     * @param logger       an external logger.
     */
    public DeepMAR(int gpu,
                   @Nonnull String protocolPath,
                   @Nonnull String weightsPath,
                   @Nullable Logger logger) {
        super(gpu, protocolPath, weightsPath, logger);
    }

    /**
     * Create an instance of DeepMAR. The protocol and weights are retrieved from the JAR.
     *
     * @param gpu    index of GPU to use.
     * @param logger logger for outputting debug info.
     */
    public DeepMAR(int gpu,
                   @Nullable Logger logger) throws IOException {
        super(gpu, getFilePath(tempProtocolFile), getFilePath(tempWeightsFile), logger);
    }

    private static String getFilePath(@Nullable File file) throws FileNotFoundException {
        if (file == null) {
            throw new FileNotFoundException("Cannot found default Caffe protocol or weights file in the JAR package.");
        } else {
            return file.getPath();
        }
    }

    /**
     * Recognize attributes from a track of pedestrian.
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
        // Process image.
        opencv_core.Mat image = new opencv_core.Mat(bbox.height, bbox.width, CV_8UC3);
        image.data(new BytePointer(bbox.patchData));
        image.convertTo(image, CV_32FC3);
        opencv_imgproc.resize(image, image, new opencv_core.Size(INPUT_WIDTH, INPUT_HEIGHT));

        // Regularize pixel values.
        final int numPixelPerChannel = image.rows() * image.cols();
        final int numPixels = numPixelPerChannel * 3;
        final FloatPointer floatDataPointer = new FloatPointer(image.data());

//        float[] origin = new float[numPixels];
//        floatDataPointer.getTracklet(origin);
//        for (int i = 0; i < numPixels; ++i) {
//            origin[i] = (origin[i] - 128) / 256;
//        }
//        floatDataPointer.put(origin);
        // Subtract mean pixel.
        sub32f(floatDataPointer, // Pointer to minuends
                4, // Bytes per step (4 bytes for float)
                pMean, // Pointer to subtrahend
                0, // Bytes per step (using the value 128 circularly)
                floatDataPointer, // Pointer to result buffer.
                4, // Bytes per step (4 bytes for float)
                1, numPixels, // Data dimensions.
                null);
        // Regularize to -0.5 to 0.5. The additional scaling is disabled (set to 1).
        mul32f(floatDataPointer, 4, pRegCoeff, 0, floatDataPointer, 4, 1, numPixels, pScale);

        //Slice into channels.
        MatVector bgr = new MatVector(3);
        split(image, bgr);
        // Get pixel data by channel.
        final float[] pixelFloats = new float[numPixelPerChannel * 3];
        for (int i = 0; i < 3; ++i) {
            final FloatPointer fp = new FloatPointer(bgr.get(i).data());
            fp.get(pixelFloats, i * numPixelPerChannel, numPixelPerChannel);
        }

        // Put the data into the data blob.
        final caffe.FloatBlob dataBlob = net.blob_by_name("data");
        dataBlob.Reshape(1, 3, INPUT_HEIGHT, INPUT_WIDTH);
//        dataBlob.set_cpu_data(pixelFloats); // Seems like there is some bugs with this function.
        dataBlob.cpu_data().put(pixelFloats);

        // Forward the data.
        net.Forward();

        // Transform result to Attributes and return.
        return fillAttributes(net.blob_by_name("fc8"));
    }

    @Nonnull
    private Attributes fillAttributes(@Nonnull caffe.FloatBlob outputBlob) {
        final float[] outputArray = new float[outputBlob.count()];
        outputBlob.cpu_data().get(outputArray);

        int iter = 0;
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append('{');
        for (String attr : ATTR_LIST) {
            jsonBuilder.append('\"').append(attr).append('\"').append('=').append(outputArray[iter++]);
            if (iter < ATTR_LIST.length) {
                jsonBuilder.append(',');
            }
        }
        jsonBuilder.append('}');
        assert iter == ATTR_LIST.length;

        return new Gson().fromJson(jsonBuilder.toString(), Attributes.class);
    }
}
