package org.cripac.isee.pedestrian.attr;/***********************************************************************
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

import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.opencv_core;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

import static org.bytedeco.javacpp.caffe.TEST;
import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_highgui.imshow;
import static org.bytedeco.javacpp.opencv_highgui.waitKey;

/**
 * The class DeepMAR uses the algorithm proposed in
 * Li, D., Chen, X., Huang, K.:
 * Multi-attribute learning for pedestrian attribute recognition in surveillance scenarios. In: Proc. ACPR (2015)
 * to conduct pedestrian attribute recognizing.
 * <p>
 * Created by ken.yu on 17-1-10.
 */
public final class DeepMAR extends PedestrianAttrRecognizer {
    private final static FloatPointer pMean;
    private final static FloatPointer pRegCoeff;
    private final static DoublePointer pScale;

    private final static float MEAN_PIXEL = 128;
    private final static float REG_COEFF = 1.0f / 256;

    static {
        Loader.load(opencv_core.class);
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

    /**
     * Instance of DeepMAR.
     */
    private caffe.FloatNet net = null;

    private final int NUM_ATTR = 125;
    private final int INPUT_WIDTH = 227;
    private final int INPUT_HEIGHT = 227;
    private final Logger logger;

    /**
     * Create an instance of DeepMAR.
     *
     * @param gpu    The GPU to use.
     * @param logger An external logger.
     */
    public DeepMAR(int gpu,
                   @Nonnull String protocolPath,
                   @Nonnull String weightsPath,
                   @Nullable Logger logger) {
        if (logger == null) {
            this.logger = new ConsoleLogger();
        } else {
            this.logger = logger;
        }

        if (gpu >= 0) {
            this.logger.info("Use GPU with device ID " + gpu);
            caffe.Caffe.SetDevice(gpu);
            caffe.Caffe.set_mode(caffe.Caffe.GPU);
        } else {
            this.logger.info("Use CPU.");
            caffe.Caffe.set_mode(caffe.Caffe.CPU);
        }

        net = new caffe.FloatNet(protocolPath, TEST);
        net.CopyTrainedLayersFrom(weightsPath);
        this.logger.debug("DeepMAR initialized!");
    }

    // TODO: Delete debug codes.
    private boolean floatEqual(float a, float b) {
        return Math.abs(a - b) < 0.000001;
    }

    /**
     * Recognize attributes from a track of pedestrian.
     *
     * @param tracklet A pedestrian track.
     * @return The attributes of the pedestrian specified by the track.
     * @throws IOException Exception that might occur during recognition.
     */
    @Override
    public Attributes recognize(@Nonnull Tracklet tracklet) throws IOException {
        // Process image.
        final Tracklet.BoundingBox bbox = tracklet.locationSequence[tracklet.locationSequence.length >> 1];
        opencv_core.Mat image = new opencv_core.Mat(bbox.height, bbox.width, CV_8UC3);
        image.data(new BytePointer(bbox.patchData));

        opencv_imgproc.resize(image, image, new opencv_core.Size(INPUT_WIDTH, INPUT_HEIGHT));

        imshow("Image", image);
        waitKey(0);

        image.convertTo(image, CV_32FC3);

        // Regularize pixel values.
        final int numPixelPerChannel = image.rows() * image.cols();
        final int numPixels = numPixelPerChannel * 3;
        final FloatPointer floatDataPointer = new FloatPointer(image.data());

        // TODO: Delete debug codes.
        final float[] origin = new float[numPixels];
        floatDataPointer.get(origin);

        sub32f(floatDataPointer, 4,
                pMean, 0,
                floatDataPointer, 4,
                1, numPixels, floatDataPointer);

        // TODO: Delete debug codes.
        for (int i = 0; i < numPixels; ++i) {
            if (!floatEqual(floatDataPointer.get(i),
                    origin[i] - MEAN_PIXEL)) {
                System.out.printf("Different after subtracting mean at %d: %f vs %f\n", i,
                        floatDataPointer.get(i),
                        origin[i] - MEAN_PIXEL);
                System.exit(-1);
            }
        }
        this.logger.debug("Mean pixel subtracted!");

        mul32f(floatDataPointer, 4,
                pRegCoeff, 0,
                floatDataPointer, 4,
                1, numPixels, pScale);

        // TODO: Delete debug codes.
        for (int i = 0; i < numPixels; ++i) {
            if (!floatEqual(floatDataPointer.get(i),
                    (origin[i] - MEAN_PIXEL) * REG_COEFF)) {
                System.out.printf("Different after regularization at %d: %f vs %f\n", i,
                        floatDataPointer.get(i),
                        (origin[i] - MEAN_PIXEL) * REG_COEFF);
                System.exit(-1);
            }
        }
        this.logger.debug("Pixels regularized!");

//        image = image.t().asMat().clone();

        //Slice into channels.
        MatVector bgr = new MatVector(3);
        split(image, bgr);
        // Get pixel data by channel.
        final float[] pixelFloats = new float[numPixelPerChannel * 3];
        for (int i = 0; i < 3; ++i) {
            final FloatPointer fp = new FloatPointer(bgr.get(i).data());
            fp.get(pixelFloats, i * numPixelPerChannel, numPixelPerChannel);

            // TODO: Delete debug codes.
            for (int j = 0; j < numPixelPerChannel; ++j) {
                if (!floatEqual(pixelFloats[i * numPixelPerChannel + j],
                        (origin[j * 3 + i] - MEAN_PIXEL) * REG_COEFF)) {
                    System.out.printf("Different after slicing channels at %d,%d: %f vs %f\n", i, j,
                            pixelFloats[i * numPixelPerChannel + j],
                            (origin[j * 3 + i] - MEAN_PIXEL) * REG_COEFF);
                    System.exit(-1);
                }
            }
        }
        this.logger.debug("Channels split!");

        // Prepare input blob.
        final caffe.FloatBlob dataBlob = new caffe.FloatBlob(1, 3, INPUT_HEIGHT, INPUT_WIDTH);
        dataBlob.set_cpu_data(pixelFloats);

        // TODO: Delete debug codes.
        for (int i = 0; i < 3; ++i) {
            for (int j = 0; j < numPixelPerChannel; ++j) {
                if (!floatEqual(dataBlob.cpu_data().get(i * numPixelPerChannel + j),
                        (origin[j * 3 + i] - MEAN_PIXEL) * REG_COEFF)) {
                    System.out.printf("Different after inserting into blob at %d,%d: %f vs %f\n", i, j,
                            dataBlob.cpu_data().get(i * numPixelPerChannel + j),
                            (origin[j * 3 + i] - MEAN_PIXEL) * REG_COEFF);
//                    System.exit(-1);
                }
            }
        }
        this.logger.debug("Input blob prepared!");

        final caffe.FloatBlobVector bottomVec = new caffe.FloatBlobVector(1);
        bottomVec.put(0, dataBlob);
        // TODO: Delete debug codes.
        for (int i = 0; i < 3; ++i) {
            for (int j = 0; j < numPixelPerChannel; ++j) {
                if (!floatEqual(bottomVec.get(0).cpu_data().get(i * numPixelPerChannel + j),
                        (origin[j * 3 + i] - MEAN_PIXEL) * REG_COEFF)) {
                    System.out.printf("Different after inserting into blob vector at %d,%d: %f vs %f\n", i, j,
                            bottomVec.get(0).cpu_data().get(i * numPixelPerChannel + j),
                            (origin[j * 3 + i] - MEAN_PIXEL) * REG_COEFF);
//                    System.exit(-1);
                }
            }
        }
        this.logger.debug("Channels split!");

        // Forward the input blob and get result.
        this.logger.debug("To forward!");
        net.Forward(bottomVec);
//        net.Forward(pixelFloats);
        this.logger.debug("Forwarded!");
        final FloatPointer p = net.blob_by_name("fc8").cpu_data();

        // Transform result to Attributes and return.
        return fillAttributes(p);
    }

    private Attributes fillAttributes(FloatPointer p) {
        Attributes attributes = new Attributes();

        int iter = 0;
        attributes.genderMale = p.get(iter++);
        attributes.genderFemale = p.get(iter++);
        attributes.genderOther = p.get(iter++);
        attributes.ageSixteen = p.get(iter++);
        attributes.ageThirty = p.get(iter++);
        attributes.ageFortyFive = p.get(iter++);
        attributes.ageSixty = p.get(iter++);
        attributes.ageOlderSixty = p.get(iter++);
        attributes.weightVeryFat = p.get(iter++);
        attributes.weightLittleFat = p.get(iter++);
        attributes.weightNormal = p.get(iter++);
        attributes.weightLittleThin = p.get(iter++);
        attributes.weightVeryThin = p.get(iter++);
        attributes.roleClient = p.get(iter++);
        attributes.roleUniform = p.get(iter++);
        attributes.hairStyleNull = p.get(iter++);
        attributes.hairStyleLong = p.get(iter++);
        attributes.headShoulderBlackHair = p.get(iter++);
        attributes.headShoulderWithHat = p.get(iter++);
        attributes.headShoulderGlasses = p.get(iter++);
        attributes.headShoulderSunglasses = p.get(iter++);
        attributes.headShoulderScarf = p.get(iter++);
        attributes.headShoulderMask = p.get(iter++);
        attributes.upperShirt = p.get(iter++);
        attributes.upperSweater = p.get(iter++);
        attributes.upperVest = p.get(iter++);
        attributes.upperTshirt = p.get(iter++);
        attributes.upperCotton = p.get(iter++);
        attributes.upperJacket = p.get(iter++);
        attributes.upperSuit = p.get(iter++);
        attributes.upperHoodie = p.get(iter++);
        attributes.upperCotta = p.get(iter++);
        attributes.upperOther = p.get(iter++);
        attributes.upperBlack = p.get(iter++);
        attributes.upperWhite = p.get(iter++);
        attributes.upperGray = p.get(iter++);
        attributes.upperRed = p.get(iter++);
        attributes.upperGreen = p.get(iter++);
        attributes.upperBlue = p.get(iter++);
        attributes.upperSilvery = p.get(iter++);
        attributes.upperYellow = p.get(iter++);
        attributes.upperBrown = p.get(iter++);
        attributes.upperPurple = p.get(iter++);
        attributes.upperPink = p.get(iter++);
        attributes.upperOrange = p.get(iter++);
        attributes.upperMixColor = p.get(iter++);
        attributes.upperOtherColor = p.get(iter++);
        attributes.lowerPants = p.get(iter++);
        attributes.lowerShortPants = p.get(iter++);
        attributes.lowerSkirt = p.get(iter++);
        attributes.lowerShortSkirt = p.get(iter++);
        attributes.lowerLongSkirt = p.get(iter++);
        attributes.lowerOnePiece = p.get(iter++);
        attributes.lowerJean = p.get(iter++);
        attributes.lowerTightPants = p.get(iter++);
        attributes.lowerBlack = p.get(iter++);
        attributes.lowerWhite = p.get(iter++);
        attributes.lowerGray = p.get(iter++);
        attributes.lowerRed = p.get(iter++);
        attributes.lowerGreen = p.get(iter++);
        attributes.lowerBlue = p.get(iter++);
        attributes.lowerSilver = p.get(iter++);
        attributes.lowerYellow = p.get(iter++);
        attributes.lowerBrown = p.get(iter++);
        attributes.lowerPurple = p.get(iter++);
        attributes.lowerPink = p.get(iter++);
        attributes.lowerOrange = p.get(iter++);
        attributes.lowerMixColor = p.get(iter++);
        attributes.lowerOtherColor = p.get(iter++);
        attributes.shoesLeather = p.get(iter++);
        attributes.shoesSport = p.get(iter++);
        attributes.shoesBoot = p.get(iter++);
        attributes.shoesCloth = p.get(iter++);
        attributes.shoesShandle = p.get(iter++);
        attributes.shoesCasual = p.get(iter++);
        attributes.shoesOther = p.get(iter++);
        attributes.shoesBlack = p.get(iter++);
        attributes.shoesWhite = p.get(iter++);
        attributes.shoesGray = p.get(iter++);
        attributes.shoesRed = p.get(iter++);
        attributes.shoesGreen = p.get(iter++);
        attributes.shoesBlue = p.get(iter++);
        attributes.shoesSilver = p.get(iter++);
        attributes.shoesYellow = p.get(iter++);
        attributes.shoesBrown = p.get(iter++);
        attributes.shoesPurple = p.get(iter++);
        attributes.shoesPink = p.get(iter++);
        attributes.shoesOrange = p.get(iter++);
        attributes.shoesMixColor = p.get(iter++);
        attributes.shoesOtherColor = p.get(iter++);
        attributes.accessoryBackpack = p.get(iter++);
        attributes.accessoryShoulderBag = p.get(iter++);
        attributes.accessoryHandbag = p.get(iter++);
        attributes.accessoryWaistBag = p.get(iter++);
        attributes.accessoryBox = p.get(iter++);
        attributes.accessoryPlasticBag = p.get(iter++);
        attributes.accessoryPaperBag = p.get(iter++);
        attributes.accessoryCart = p.get(iter++);
        attributes.accessoryKid = p.get(iter++);
        attributes.accessoryOther = p.get(iter++);
        attributes.actionCalling = p.get(iter++);
        attributes.actionArmStretching = p.get(iter++);
        attributes.actionChatting = p.get(iter++);
        attributes.actionGathering = p.get(iter++);
        attributes.actionLying = p.get(iter++);
        attributes.actionCrouching = p.get(iter++);
        attributes.actionRunning = p.get(iter++);
        attributes.actionHoldThing = p.get(iter++);
        attributes.actionPushing = p.get(iter++);
        attributes.actionPulling = p.get(iter++);
        attributes.actionNipThing = p.get(iter++);
        attributes.actionPicking = p.get(iter++);
        attributes.actionOther = p.get(iter++);
        attributes.viewAngleLeft = p.get(iter++);
        attributes.viewAngleRight = p.get(iter++);
        attributes.viewAngleFront = p.get(iter++);
        attributes.viewAngleBack = p.get(iter++);
        attributes.occlusionLeft = p.get(iter++);
        attributes.occlusionRight = p.get(iter++);
        attributes.occlusionUp = p.get(iter++);
        attributes.occlusionDown = p.get(iter++);
        attributes.occlusionEnvironment = p.get(iter++);
        attributes.occlusionAccessory = p.get(iter++);
        attributes.occlusionObject = p.get(iter++);
        attributes.occlusionOther = p.get(iter++);
        assert iter == NUM_ATTR;

        return attributes;
    }
}
