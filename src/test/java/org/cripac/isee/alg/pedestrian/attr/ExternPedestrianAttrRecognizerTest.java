package org.cripac.isee.alg.pedestrian.attr;

import com.google.gson.Gson;
import org.apache.log4j.Level;
import org.bytedeco.javacpp.opencv_core;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;

import static org.bytedeco.javacpp.opencv_imgcodecs.imread;
import static org.junit.Assert.assertEquals;
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

/**
 * Created by ken.yu on 17-1-11.
 */
public class ExternPedestrianAttrRecognizerTest {
    final Logger logger = new ConsoleLogger(Level.DEBUG);
    static PedestrianAttrRecognizer recognizer;
    static String testImage;
    static Attributes answer;

    @Before
    public void setUp() throws IOException {
        recognizer = new ExternPedestrianAttrRecognizer(
                InetAddress.getByName("172.18.33.90"), 8500, logger
        );
        testImage = "src/test/resources/" +
                "CAM01_2014-02-15_20140215161032-20140215162620_tarid0_frame218_line1.png";
        answer = new Gson().fromJson("{\"action_pulling\": -0.17966261506080627, \"lower_green\": -0.10017161816358566, \"gender_female\": -0.20716792345046997, \"upper_cotton\": -0.24999018013477325, \"accessory_other\": 0.026112157851457596, \"occlusion_accessory\": 0.047553468495607376, \"upper_suit\": 0.15522423386573792, \"shoes_casual\": -0.0837176963686943, \"shoes_white\": -0.10278059542179108, \"lower_pants\": -0.154108464717865, \"shoes_boot\": -0.1702422946691513, \"age_60\": -0.2689493000507355, \"accessory_backpack\": -0.005006758496165276, \"head_shoulder_mask\": -0.021039944142103195, \"upper_vest\": -0.08472561836242676, \"lower_white\": -0.04942454770207405, \"upper_black\": -0.04958377033472061, \"upper_white\": -0.14260771870613098, \"upper_shirt\": 0.015975749120116234, \"upper_silvery\": -0.12476518005132675, \"role_client\": -0.03333559259772301, \"upper_brown\": 0.10534601658582687, \"action_nipthing\": 0.04711170122027397, \"shoes_silver\": 0.06653876602649689, \"accessory_waistbag\": -0.15774032473564148, \"lower_short_skirt\": -0.0412132628262043, \"action_picking\": -0.02799537032842636, \"shoes_black\": -0.10529618710279465, \"occlusion_down\": -0.07037542015314102, \"shoes_yellow\": -0.28258490562438965, \"gender_other\": -0.19252020120620728, \"accessory_shoulderbag\": -0.0791037380695343, \"upper_cotta\": -0.1090533435344696, \"occlusion_right\": -0.1262543797492981, \"action_pushing\": 0.060128919780254364, \"shoes_green\": -0.18958471715450287, \"action_armstretching\": -0.12325656414031982, \"shoes_other\": -0.15972185134887695, \"shoes_red\": -0.1309235394001007, \"lower_mix_color\": -0.1747373342514038, \"occlusion_left\": -0.08904217183589935, \"view_angle_left\": 0.057984739542007446, \"shoes_sport\": -0.05745989829301834, \"lower_gray\": -0.26319101452827454, \"upper_other\": 0.005861850455403328, \"lower_yellow\": 0.19873106479644775, \"head_shoulder_sunglasses\": -0.03430885076522827, \"upper_tshirt\": -0.03488291800022125, \"accessory_cart\": -0.12084956467151642, \"age_16\": -0.05667847394943237, \"hair_style_null\": 7.888674736022949e-05, \"upper_hoodie\": -0.2319737672805786, \"shoes_mix_color\": 0.17251841723918915, \"upper_green\": 0.0564429797232151, \"age_older_60\": -0.0765448808670044, \"shoes_cloth\": -0.02858760394155979, \"action_chatting\": 0.041459228843450546, \"shoes_purple\": 0.1992325782775879, \"upper_other_color\": -0.14060889184474945, \"lower_black\": 0.004167519509792328, \"lower_tight_pants\": 0.11908357590436935, \"action_holdthing\": -0.06531824171543121, \"lower_pink\": -0.07166054099798203, \"action_other\": 0.21119438111782074, \"upper_orange\": 0.09643063694238663, \"lower_jean\": -0.04210019111633301, \"hair_style_long\": 0.031678300350904465, \"upper_red\": -0.04058532416820526, \"lower_silver\": -0.1368672251701355, \"lower_short_pants\": -0.09898153692483902, \"occlusion_up\": 0.03863256052136421, \"lower_blue\": -0.05066308751702309, \"upper_purple\": -0.14978016912937164, \"upper_pink\": 0.03800048679113388, \"shoes_pink\": 0.1257370263338089, \"shoes_shandle\": 0.13704988360404968, \"shoes_leather\": 0.0037415758706629276, \"occlusion_environment\": -0.03722403198480606, \"view_angle_right\": -0.030682001262903214, \"shoes_other_color\": -0.15661853551864624, \"lower_one_piece\": -0.13335086405277252, \"head_shoulder_with_hat\": -0.23638661205768585, \"age_30\": -0.14755187928676605, \"shoes_gray\": -0.10813336819410324, \"accessory_plasticbag\": -0.2885738015174866, \"role_uniform\": -0.0036274129524827003, \"shoes_brown\": -0.10026665031909943, \"action_crouching\": -0.2668699026107788, \"lower_purple\": -0.10168711841106415, \"weight_very_thin\": -0.3678150773048401, \"shoes_blue\": -0.07561597228050232, \"weight_normal\": -0.11945008486509323, \"action_running\": -0.10827206075191498, \"view_angle_front\": -0.20001788437366486, \"accessory_paperbag\": -0.19929121434688568, \"head_shoulder_black_hair\": -0.2148313820362091, \"accessory_box\": -0.1550966501235962, \"lower_long_skirt\": -0.17501309514045715, \"shoes_orange\": -0.11892542988061905, \"weight_little_fat\": -0.07645877450704575, \"action_lying\": 0.12984241545200348, \"lower_other_color\": -0.046849772334098816, \"upper_jacket\": 0.08519292622804642, \"upper_blue\": 0.023207101970911026, \"lower_orange\": 0.008142034523189068, \"upper_gray\": -0.2650142312049866, \"accessory_handbag\": -0.08740431070327759, \"age_45\": -0.0238933227956295, \"lower_skirt\": -0.03608536347746849, \"upper_sweater\": -0.0705333948135376, \"lower_brown\": -0.08255314826965332, \"accessory_kid\": -0.08497394621372223, \"occlusion_object\": 0.1716439574956894, \"head_shoulder_scarf\": 0.030656255781650543, \"gender_male\": 0.09624288231134415, \"action_gathering\": -0.14736978709697723, \"lower_red\": -0.12097720056772232, \"action_calling\": -0.21006231009960175, \"head_shoulder_glasses\": -0.03936345875263214, \"upper_mix_color\": -0.06711417436599731, \"view_angle_back\": -0.06884464621543884, \"upper_yellow\": -0.0469282791018486, \"weight_very_fat\": -0.1198916882276535, \"weight_little_thin\": -0.1745450794696808, \"occlusion_other\": 0.02750432677567005}",
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

    //    @Test
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