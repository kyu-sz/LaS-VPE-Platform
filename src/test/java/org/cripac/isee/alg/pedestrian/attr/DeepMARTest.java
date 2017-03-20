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
import org.apache.log4j.Level;
import org.bytedeco.javacpp.opencv_core;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.junit.Before;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.AccessDeniedException;

import static org.bytedeco.javacpp.opencv_imgcodecs.imread;
import static org.junit.Assert.assertEquals;

/**
 * Created by ken.yu on 17-1-11.
 */
public class DeepMARTest {
    private final static File protocol = new File("models/DeepMAR/DeepMAR.prototxt");
    private final static File model = new File("models/DeepMAR/DeepMAR.caffemodel");

    private static PedestrianAttrRecognizer recognizer;
    private static String testImage;
    private static Attributes answer;

    final Logger logger = new ConsoleLogger(Level.DEBUG);
    final int caffeGPU;

    public DeepMARTest(int caffeGPU) {
        this.caffeGPU = caffeGPU;
    }

    @Before
    public void setUp() throws FileNotFoundException, AccessDeniedException {
        recognizer = new DeepMAR(caffeGPU, protocol, model, logger);
        testImage = "src/test/resources/" +
                "CAM01_2014-02-15_20140215161032-20140215162620_tarid0_frame218_line1.png";
        answer = new Gson().fromJson("{\"action_pulling\": -0.17966261506080627, \"lower_green\": -0.10017161816358566, \"gender_female\": -0.20716792345046997, \"upper_cotton\": -0.24999018013477325, \"accessory_other\": 0.026112159714102745, \"occlusion_accessory\": 0.047553468495607376, \"upper_suit\": 0.15522421896457672, \"shoes_casual\": -0.08371768891811371, \"shoes_white\": -0.10278059542179108, \"lower_pants\": -0.154108464717865, \"shoes_boot\": -0.1702422946691513, \"age_60\": -0.2689493000507355, \"accessory_backpack\": -0.005006761290132999, \"head_shoulder_mask\": -0.0210399366915226, \"upper_vest\": -0.08472561836242676, \"lower_white\": -0.04942454397678375, \"upper_black\": -0.04958377033472061, \"upper_white\": -0.1426077038049698, \"upper_shirt\": 0.01597575470805168, \"upper_silvery\": -0.12476517260074615, \"role_client\": -0.03333559259772301, \"upper_brown\": 0.10534601658582687, \"action_nipthing\": 0.04711170494556427, \"shoes_silver\": 0.06653877347707748, \"accessory_waistbag\": -0.15774032473564148, \"lower_short_skirt\": -0.0412132665514946, \"action_picking\": -0.02799537591636181, \"shoes_black\": -0.10529618710279465, \"occlusion_down\": -0.07037542760372162, \"shoes_yellow\": -0.28258490562438965, \"gender_other\": -0.19252020120620728, \"accessory_shoulderbag\": -0.0791037380695343, \"upper_cotta\": -0.1090533509850502, \"occlusion_right\": -0.1262543797492981, \"action_pushing\": 0.060128916054964066, \"shoes_green\": -0.18958471715450287, \"action_armstretching\": -0.12325657159090042, \"shoes_other\": -0.15972183644771576, \"shoes_red\": -0.1309235394001007, \"lower_mix_color\": -0.1747373342514038, \"occlusion_left\": -0.08904216438531876, \"view_angle_left\": 0.05798473581671715, \"shoes_sport\": -0.05745989456772804, \"lower_gray\": -0.26319101452827454, \"upper_other\": 0.005861848127096891, \"lower_yellow\": 0.19873106479644775, \"head_shoulder_sunglasses\": -0.034308843314647675, \"upper_tshirt\": -0.03488292172551155, \"accessory_cart\": -0.12084956467151642, \"age_16\": -0.056678466498851776, \"hair_style_null\": 7.889792323112488e-05, \"upper_hoodie\": -0.2319737821817398, \"shoes_mix_color\": 0.17251840233802795, \"upper_green\": 0.0564429797232151, \"age_older_60\": -0.0765448734164238, \"shoes_cloth\": -0.028587602078914642, \"action_chatting\": 0.041459228843450546, \"shoes_purple\": 0.1992325782775879, \"upper_other_color\": -0.14060887694358826, \"lower_black\": 0.004167518578469753, \"lower_tight_pants\": 0.11908358335494995, \"action_holdthing\": -0.06531824916601181, \"lower_pink\": -0.07166054844856262, \"action_other\": 0.21119438111782074, \"upper_orange\": 0.09643063694238663, \"lower_jean\": -0.04210019111633301, \"hair_style_long\": 0.03167831152677536, \"upper_red\": -0.04058533161878586, \"lower_silver\": -0.1368672400712967, \"lower_short_pants\": -0.09898153692483902, \"occlusion_up\": 0.038632556796073914, \"lower_blue\": -0.05066308751702309, \"upper_purple\": -0.14978016912937164, \"upper_pink\": 0.03800048679113388, \"shoes_pink\": 0.1257370412349701, \"shoes_shandle\": 0.13704988360404968, \"shoes_leather\": 0.0037415758706629276, \"occlusion_environment\": -0.03722403571009636, \"view_angle_right\": -0.030682001262903214, \"shoes_other_color\": -0.15661853551864624, \"lower_one_piece\": -0.13335086405277252, \"head_shoulder_with_hat\": -0.23638662695884705, \"age_30\": -0.14755186438560486, \"shoes_gray\": -0.10813336074352264, \"accessory_plasticbag\": -0.2885737717151642, \"role_uniform\": -0.0036274101585149765, \"shoes_brown\": -0.10026665031909943, \"action_crouching\": -0.2668699026107788, \"lower_purple\": -0.10168711096048355, \"weight_very_thin\": -0.3678150475025177, \"shoes_blue\": -0.07561597228050232, \"weight_normal\": -0.11945008486509323, \"action_running\": -0.10827205330133438, \"view_angle_front\": -0.20001788437366486, \"accessory_paperbag\": -0.19929122924804688, \"head_shoulder_black_hair\": -0.2148313820362091, \"accessory_box\": -0.1550966501235962, \"lower_long_skirt\": -0.17501309514045715, \"shoes_orange\": -0.11892542988061905, \"weight_little_fat\": -0.07645878195762634, \"action_lying\": 0.12984243035316467, \"lower_other_color\": -0.046849776059389114, \"upper_jacket\": 0.08519292622804642, \"upper_blue\": 0.023207103833556175, \"lower_orange\": 0.008142032660543919, \"upper_gray\": -0.2650142312049866, \"accessory_handbag\": -0.08740431815385818, \"age_45\": -0.023893319070339203, \"lower_skirt\": -0.03608536720275879, \"upper_sweater\": -0.0705334022641182, \"lower_brown\": -0.08255315572023392, \"accessory_kid\": -0.08497394621372223, \"occlusion_object\": 0.1716439574956894, \"head_shoulder_scarf\": 0.030656250193715096, \"gender_male\": 0.09624288231134415, \"action_gathering\": -0.14736978709697723, \"lower_red\": -0.12097720801830292, \"action_calling\": -0.21006232500076294, \"head_shoulder_glasses\": -0.03936346247792244, \"upper_mix_color\": -0.06711417436599731, \"view_angle_back\": -0.06884464621543884, \"upper_yellow\": -0.0469282828271389, \"weight_very_fat\": -0.1198917031288147, \"weight_little_thin\": -0.1745450794696808, \"occlusion_other\": 0.027504336088895798}",
                Attributes.class);
    }

    public static Tracklet img2Tracklet(opencv_core.Mat img) {
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