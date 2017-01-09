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

package org.cripac.isee.vpe.util;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;
import org.bytedeco.javacv.OpenCVFrameConverter;

import java.io.FileInputStream;
import java.io.IOException;

import static org.bytedeco.javacpp.avutil.AV_LOG_QUIET;
import static org.bytedeco.javacpp.avutil.av_log_set_level;
import static org.bytedeco.javacpp.opencv_highgui.*;

/**
 * Created by ken.yu on 17-1-9.
 */
public class FFmpegFrameGrabberNewTest {

    private final static String videoPath = "src/test/resources/20131220184349-20131220184937.h264";

    //    @Test
    public void grabImage() throws IOException, FrameGrabber.Exception {

        System.out.println("Reading video...");

        FFmpegFrameGrabberNew decoder = new FFmpegFrameGrabberNew(new FileInputStream(videoPath));
        av_log_set_level(AV_LOG_QUIET);
        decoder.start();
        opencv_core.Mat cvFrame;
        int frameCnt = 0;
        boolean display = true;
        while (true) {
            Frame frame = decoder.grabImage();
            if (frame == null) {
                break;
            }
            ++frameCnt;
            if (frameCnt % 1000 == 0) {
                System.out.println("Decoded " + frameCnt + " frames!");
            }
            if (display) {
                cvFrame = new OpenCVFrameConverter.ToMat().convert(frame);
                imshow("VideoDecoder", cvFrame);
                int key_pressed = waitKey(1);
                if ((key_pressed & ((1 << 8) - 1)) == ' ') {
                    System.out.print("Stop displaying, but continue to decode for counting frames...\n");
                    destroyAllWindows();
                    display = false;
                }
            }
        }
        assert frameCnt == 4350;
    }
}