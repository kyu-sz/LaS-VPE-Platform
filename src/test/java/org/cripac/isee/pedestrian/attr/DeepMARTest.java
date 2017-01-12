package org.cripac.isee.pedestrian.attr;

import org.apache.log4j.Level;
import org.bytedeco.javacpp.opencv_core;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.junit.Test;

import static org.bytedeco.javacpp.opencv_imgcodecs.imread;
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
    final String protocolPath = "src/main/resources/models/DeepMAR/DeepMAR.prototxt";
    final String weightsPath = "src/main/resources/models/DeepMAR/DeepMAR.caffemodel";

    final Logger logger = new ConsoleLogger(Level.DEBUG);
    final DeepMAR recognizer = new DeepMAR(-1, protocolPath, weightsPath, logger);

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
        final String testImage = "src/test/resources/" +
                "CAM01_2014-02-15_20140215161032-20140215162620_tarid0_frame218_line1.png";
        final String testImage1 = "/home/ken.yu/Pictures/sample.jpg";
        Attributes attributes;
        attributes = recognizer.recognize(img2Tracklet(imread(testImage)));
        logger.info(attributes);
        attributes = recognizer.recognize(img2Tracklet(imread(testImage).t().asMat()));
        logger.info(attributes);
        attributes = recognizer.recognize(img2Tracklet(imread(testImage)));
        logger.info(attributes);
        attributes = recognizer.recognize(img2Tracklet(imread(testImage)));
        logger.info(attributes);
        attributes = recognizer.recognize(img2Tracklet(imread(testImage1)));
        logger.info(attributes);
    }
}