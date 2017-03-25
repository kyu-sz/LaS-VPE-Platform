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
 * Created by ken.yu on 17-3-25.
 */
package org.cripac.isee.alg.pedestrian.attr;

import com.google.gson.Gson;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.FloatPointer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;

import javax.annotation.Nonnull;

import static org.bytedeco.javacpp.opencv_core.*;

public interface DeepMAR extends PedestrianAttrRecognizer {
    float MEAN_PIXEL = 128;
    float REG_COEFF = 1.0f / 256;

    FloatPointer pMean = new FloatPointer(MEAN_PIXEL);
    FloatPointer pRegCoeff = new FloatPointer(REG_COEFF);
    FloatPointer pScale = new FloatPointer(1.f);

    int INPUT_WIDTH = 227;
    int INPUT_HEIGHT = 227;

    static float[] pixelFloatsFromBBox(Tracklet.BoundingBox bbox) {
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

        return pixelFloats;
    }

    @Nonnull
    static Attributes fillAttributes(@Nonnull float[] outputArray) {
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

    String[] ATTR_LIST = new String[]{
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
