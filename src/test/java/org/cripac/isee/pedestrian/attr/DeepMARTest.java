package org.cripac.isee.pedestrian.attr;

import com.google.gson.Gson;
import org.apache.log4j.Level;
import org.bytedeco.javacpp.opencv_core;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.junit.Before;
import org.junit.Test;

import static org.bytedeco.javacpp.opencv_imgcodecs.imread;
import static org.junit.Assert.assertEquals;
/***********************************************************************
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
 ************************************************************************/

/**
 * Created by ken.yu on 17-1-11.
 */
public class DeepMARTest {
    final static String protocolPath = "src/main/resources/models/DeepMAR/DeepMAR.prototxt";
    final static String weightsPath = "src/main/resources/models/DeepMAR/DeepMAR.caffemodel";

    final Logger logger = new ConsoleLogger(Level.DEBUG);
    static DeepMAR recognizer;
    static String testImage;
    static Attributes answer;

    @Before
    public void setUp() {
        recognizer = new DeepMAR(-1, protocolPath, weightsPath, logger);
        testImage = "src/test/resources/" +
                "CAM01_2014-02-15_20140215161032-20140215162620_tarid0_frame218_line1.png";
        answer = new Gson().fromJson("{\"gender_male\":0.096247636,\"gender_female\":-0.20714453,\"gender_other\":-0.19252093,\"age_16\":-0.05666464,\"age_30\":-0.1566182,\"age_45\":-0.02388296,\"age_60\":-0.26893526,\"age_older_60\":-0.0050136195,\"weight_very_fat\":-0.11991686,\"weight_little_fat\":-0.17501752,\"weight_normal\":-0.07562953,\"weight_little_thin\":-0.21007271,\"weight_very_thin\":-0.101670146,\"role_client\":-0.033336956,\"role_uniform\":-0.10812201,\"hair_style_null\":1.1440925E-4,\"hair_style_long\":-0.042111363,\"head_shoulder_black_hair\":-0.20004234,\"head_shoulder_with_hat\":-0.030681394,\"head_shoulder_glasses\":-0.039379645,\"head_shoulder_sunglasses\":-0.03429083,\"head_shoulder_scarf\":0.096430585,\"head_shoulder_mask\":-0.021014761,\"upper_shirt\":0.015996326,\"upper_sweater\":-0.070547186,\"upper_vest\":-0.08472876,\"upper_tshirt\":-0.1368788,\"upper_cotton\":-0.2500005,\"upper_jacket\":0.030639816,\"upper_suit\":-0.14058672,\"upper_hoodie\":-0.23199159,\"upper_cotta\":-0.109067634,\"upper_other\":0.0058551733,\"upper_black\":-0.049583394,\"upper_white\":-0.14258671,\"upper_gray\":-0.26500273,\"upper_red\":0.031703815,\"upper_green\":0.056440413,\"upper_blue\":-0.046862528,\"upper_silvery\":-0.12474477,\"upper_yellow\":-0.04694526,\"upper_brown\":0.10533305,\"upper_purple\":-0.050665595,\"upper_pink\":-0.03489478,\"upper_orange\":-0.071669556,\"upper_mix_color\":-0.06712864,\"upper_other_color\":0.19923341,\"lower_pants\":-0.1540865,\"lower_short_pants\":-0.003616456,\"lower_skirt\":-0.0360984,\"lower_short_skirt\":-0.08740752,\"lower_long_skirt\":-0.21481794,\"lower_one_piece\":-0.037239797,\"lower_jean\":0.21118486,\"lower_tight_pants\":0.004164437,\"lower_black\":0.15521517,\"lower_white\":-0.04941993,\"lower_gray\":-0.26319912,\"lower_red\":0.023208903,\"lower_green\":-0.10017693,\"lower_blue\":-0.13334638,\"lower_silver\":-0.04059737,\"lower_yellow\":-0.08498675,\"lower_brown\":-0.08256505,\"lower_purple\":-0.26686034,\"lower_pink\":0.038617805,\"lower_orange\":0.08519454,\"lower_mix_color\":-0.17474665,\"lower_other_color\":-0.07648746,\"shoes_leather\":0.12986138,\"shoes_sport\":-0.05745287,\"shoes_boot\":-0.17026171,\"shoes_cloth\":-0.07652131,\"shoes_shandle\":0.03800013,\"shoes_casual\":-0.08370507,\"shoes_other\":-0.15971318,\"shoes_black\":-0.10529305,\"shoes_white\":-0.10277688,\"shoes_gray\":-0.23640458,\"shoes_red\":-0.13092858,\"shoes_green\":-0.18958966,\"shoes_blue\":-0.36779386,\"shoes_silver\":0.06656195,\"shoes_yellow\":-0.28260568,\"shoes_brown\":-0.1993043,\"shoes_purple\":0.041466925,\"shoes_pink\":-0.1497701,\"shoes_orange\":-0.15511565,\"shoes_mix_color\":0.17251062,\"shoes_other_color\":0.0037400257,\"accessory_backpack\":-0.17453326,\"accessory_shoulderbag\":-0.0791041,\"accessory_handbag\":-0.041217595,\"accessory_waistbag\":-0.15771481,\"accessory_box\":-0.28856212,\"accessory_plasticbag\":-0.147541,\"accessory_paperbag\":-0.10825773,\"accessory_cart\":-0.12083508,\"accessory_kid\":0.19872172,\"accessory_other\":0.026121484,\"action_calling\":-0.12098864,\"action_armstretching\":-0.123276636,\"action_chatting\":-0.028586488,\"action_gathering\":-0.06885322,\"action_lying\":-0.11891096,\"action_crouching\":-0.10025999,\"action_running\":-0.14736053,\"action_holdthing\":0.11908882,\"action_pushing\":0.0601194,\"action_pulling\":-0.17968494,\"action_nipthing\":0.047129218,\"action_picking\":-0.028008802,\"action_other\":-0.065327175,\"view_angle_left\":0.05797565,\"view_angle_right\":0.13703503,\"view_angle_front\":-0.11944221,\"view_angle_back\":0.008136962,\"occlusion_left\":-0.089031,\"occlusion_right\":-0.12625897,\"occlusion_up\":-0.09898358,\"occlusion_down\":-0.070382066,\"occlusion_environment\":0.12578094,\"occlusion_accessory\":0.04754394,\"occlusion_object\":0.17164445,\"occlusion_other\":0.02752708}",
                Attributes.class);
    }

    static Tracklet img2Tracklet(opencv_core.Mat img) {
        final Tracklet.BoundingBox boundingBox = new Tracklet.BoundingBox();
        boundingBox.height = img.rows();
        boundingBox.width = img.cols();
        boundingBox.x = 0;
        boundingBox.y = 0;
        boundingBox.patchData = new byte[boundingBox.width * boundingBox.height * 3];
        img.data().get(boundingBox.patchData);

        final Tracklet tracklet = new Tracklet();
        tracklet.locationSequence = new Tracklet.BoundingBox[1];
        tracklet.locationSequence[0] = boundingBox;
        return tracklet;
    }

    @Test
    public void recognize() throws Exception {
        final int NUM_ROUNDS = 100;
        long timeSum = 0;
        Attributes attributes = null;
        for (int i = 0; i < NUM_ROUNDS; ++i) {
            final long start = System.currentTimeMillis();
            attributes = recognizer.recognize(img2Tracklet(imread(testImage)));
            final long end = System.currentTimeMillis();
            timeSum += end - start;
            assertEquals(answer, attributes);
        }
        logger.info((timeSum / NUM_ROUNDS) + "ms per round.");
        logger.info(attributes);
    }
}