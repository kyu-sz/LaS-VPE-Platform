package org.cripac.isee.pedestrian.attr;

import org.apache.log4j.Level;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;

import static org.bytedeco.javacpp.opencv_imgcodecs.imread;
import static org.cripac.isee.pedestrian.attr.DeepMARTest.img2Tracklet;
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
public class ExternPedestrianAttrRecognizerTest {
    final Logger logger = new ConsoleLogger(Level.DEBUG);
    final ExternPedestrianAttrRecognizer recognizer = new ExternPedestrianAttrRecognizer(
            InetAddress.getByName("172.18.33.90"), 8500, logger
    );

    public ExternPedestrianAttrRecognizerTest() throws IOException {
    }

    @Test
    public void recognize() throws Exception {
        final String testImage = "src/test/resources/" +
                "CAM01_2014-02-15_20140215161032-20140215162620_tarid0_frame218_line1.png";
        Attributes attributes;
        attributes = recognizer.recognize(img2Tracklet(imread(testImage)));
        logger.info(attributes);
        attributes = recognizer.recognize(img2Tracklet(imread(testImage)));
        logger.info(attributes);
        attributes = recognizer.recognize(img2Tracklet(imread(testImage)));
        logger.info(attributes);
        attributes = recognizer.recognize(img2Tracklet(imread(testImage)));
        logger.info(attributes);
    }
}